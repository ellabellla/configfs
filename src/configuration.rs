use std::{sync::Arc, collections::HashMap};

use fuse3::{Errno, Result};
use rand::Rng;
use tokio::sync::RwLock;

use crate::{Node, Fetch, Update};


pub struct Configuration {
    root: u64,
    nodes: HashMap<u64, Node>,
}

impl Configuration {
    pub fn new() -> Arc<RwLock<Configuration>> {
        let root = 1;
        let mut nodes = HashMap::new();
        nodes.insert(root, Node::Group(HashMap::new(), 0));
        Arc::new(RwLock::new(Configuration{nodes, root}))
    }

    fn split_path(path: &str) -> Vec<&str> {
        path.split('/').filter(|s| *s != "").collect()
    }

    fn new_ino(&mut self, node: Node) -> u64 {
        let mut rng = rand::thread_rng();
        let mut ino = rng.gen();
        while self.nodes.contains_key(&ino) {
            ino = rng.gen();
        }

        self.nodes.insert(ino, node);
        ino
    }

    pub fn get_root<'a>(&'a self) -> Result<&'a HashMap<String, u64>> {
        if let Some(Node::Group(root, _)) = self.nodes.get(&self.root) {
            Ok(root)
        } else {
            Err(Errno::new_not_exist())
        }
    }

    pub fn get_root_mut<'a>(&'a mut self) -> Result<&'a mut HashMap<String, u64>> {
        if let Some(Node::Group(root, _)) = self.nodes.get_mut(&self.root) {
            Ok(root)
        } else {
            Err(Errno::new_not_exist())
        }
    }

    pub fn find(&self, path: &str) -> Result<(u64, &Node)> {
        let nodes = Configuration::split_path(path);

        let mut group = self.get_root()?;
        for (i, name) in nodes.iter().enumerate() {
            if let Some(ino) = group.get(*name) {
                match self.nodes.get(ino) {
                    Some(node) => match node {
                        Node::Data(_, _) => if i == self.nodes.len() - 1 {
                            return Ok((*ino, node))
                        } else {
                            return Err(Errno::new_not_exist())
                        },
                        Node::Group(new_group, _) => if i == self.nodes.len() - 1 {
                            return Ok((*ino, node))
                        } else {
                            group = new_group
                        },
                    },
                    None => return Err(Errno::new_not_exist()),
                }
            } else {
                return Err(Errno::new_not_exist())
            }
        }
        return Err(Errno::new_not_exist())
    }

    pub fn fetch<'a>(&'a self, ino: u64) -> Result<&'a Node> {
        self.nodes.get(&ino).ok_or_else(|| Errno::new_not_exist())
    }

    pub fn update(&self, ino: u64, data: Vec<u8>) -> Result<()> {
        let Node::Data(_, update) = self.fetch(ino)? else {
            return Err(Errno::new_is_dir());
        };
        Ok(update(ino, data)?)
    }

    pub fn contains(&self, ino: u64, name: &str) -> Result<bool> {
        self.nodes.get(&ino)
            .ok_or_else(|| Errno::new_not_exist())
            .and_then(|group| match group {
                Node::Data(_, _) => Err(Errno::new_not_exist()),
                Node::Group(group, _) => Ok(group.contains_key(name)),
            })
    }

    pub fn get_child(&self, ino: u64, name: &str) -> Result<u64> {
        self.nodes.get(&ino)
            .ok_or_else(|| Errno::new_not_exist())
            .and_then(|group| match group {
                Node::Data(_, _) => Err(Errno::new_not_exist()),
                Node::Group(group, _) => group.get(name)
                    .map(|i| i.clone())
                    .ok_or_else(|| Errno::new_not_exist()),
            })
    }
    
    pub fn mv(&mut self, parent: u64, new_parent: u64, name: &str, new_name: &str) -> Result<()> {
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let Some(ino) = group.get(&name) {
                let ino = ino.clone();
                group.remove(&name);
                if let Some(Node::Group(group, _)) = self.nodes.get_mut(&new_parent) {
                    if let Some(ino) = group.insert(new_name.to_string(), ino) {
                        self.nodes.remove(&ino);
                    }
                    Ok(())
                } else {
                    Err(Errno::new_not_exist())
                }
            } else {
                Err(Errno::new_not_exist())
            }
        } else {
            Err(Errno::new_not_exist())
        }
    }

    pub fn rename(&mut self, parent: u64, name: &str, new_name: &str) -> Result<()> {
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let Some(ino) = group.remove(&name) {
                let ino = ino.clone();
                group.insert(new_name.to_string(), ino);
                Ok(())
            } else {
                Err(Errno::new_not_exist())
            }
        } else {
            Err(Errno::new_not_exist())
        }
    }

    pub fn create_group(&mut self, parent: u64, name: &str) -> Result<u64> {
        let ino = self.new_ino(Node::Group(HashMap::new(), parent));
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let None = group.get(&name) {
                group.insert(name, ino);
                Ok(ino)
            } else {
                self.nodes.remove(&ino);
                Err(Errno::new_exist())
            }
        } else {
            self.nodes.remove(&ino);
            Err(Errno::new_not_exist())
        }
    }

    pub fn create_file(&mut self, parent: u64, name: &str, fetch: Fetch, update: Update) -> Result<u64> {
        let ino = self.new_ino(Node::Data(fetch, update));
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let None = group.get(&name) {
                group.insert(name, ino);
                Ok(ino)
            } else {
                self.nodes.remove(&ino);
                Err(Errno::new_exist())
            }
        } else {
            self.nodes.remove(&ino);
            Err(Errno::new_not_exist())
        }
    }

    pub fn remove(&mut self, parent: u64, name: &str) -> Result<()> {
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let Some(ino) = group.get(&name) {
                let ino = ino.clone();
                group.remove(&name);
                self.nodes.remove(&ino);
                Ok(())
            } else {
                Err(Errno::new_not_exist())
            }
        } else {
            Err(Errno::new_not_exist())
        }
    }

    /*pub fn create_group(&mut self, path: &str) -> Result<()> {
        let mut nodes = Configuration::split_path(path);

        // get name of group to create
        let group_name = if let Some(name) = nodes.pop() {
            name
        } else {
            return Err(Errno::new_exist())
        };

        // temporarily add the new group and get its ino
        let ino = self.new_ino(Node::Group(HashMap::new(), 0));
        let mut last_ino = self.root;

        let mut nodes = nodes.into_iter().rev();
        let mut group = self.get_root_mut()?;
        loop {
            // get next ancestor
            let Some(name) = nodes.next() else {
                // not ancestors left, add group
                return if let None = group.get(group_name){
                    group.insert(group_name.to_string(), ino);
                    self.nodes.insert(ino, Node::Group(HashMap::new(), last_ino));
                    Ok(())
                } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
                };
            };

            // find next ancestor
            group = if let Some(ino) = group.get(name) {
                let ino = ino.clone();
                if let Some(Node::Group(group, _)) = self.nodes.get_mut(&ino) {
                    last_ino = ino;
                    Ok(group)
                } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
                }
            } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
            }?;
        }
    }*/

    /*pub fn create_file(&mut self, path: &str, fetch: Fetch, update: Update) -> Result<()> {
        let mut nodes = Configuration::split_path(path);

        // get name of group to create
        let group_name = if let Some(name) = nodes.pop() {
            name
        } else {
            return Err(Errno::new_exist())
        };

        // temporarily add the new group and get its ino
        let ino = self.new_ino(Node::Data(fetch, update));

        let mut nodes = nodes.into_iter().rev();
        let mut group = self.get_root_mut()?;
        loop {
            // get next ancestor
            let Some(name) = nodes.next() else {
                // not ancestors left, add group
                return if let None = group.get(group_name){
                    group.insert(group_name.to_string(), ino);
                    Ok(())
                } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
                };
            };

            // find next ancestor
            group = if let Some(ino) = group.get(name) {
                let ino = ino.clone();
                if let Some(Node::Group(group, _)) = self.nodes.get_mut(&ino) {
                    Ok(group)
                } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
                }
            } else {
                    // remove temp group
                    self.nodes.remove(&ino);
                    Err(Errno::new_exist())
            }?;
        }
    }

    pub fn remove(&mut self, path: &str) -> Result<()> {
        let mut nodes = Configuration::split_path(path);

        // get name of group to create
        let group_name = if let Some(name) = nodes.pop() {
            name
        } else {
            return Err(Errno::new_not_exist())
        };

        let mut inos = vec![self.root];

        let mut nodes = nodes.iter().rev();
        let mut group = self.get_root_mut()?;
        loop {
            // get next ancestor
            let Some(name) = nodes.next() else {
                return if let Some(ino) = group.get(group_name){
                    let ino = ino.clone();
                    self.nodes.remove(&ino);
                    if let Some(ino) = inos.last() {
                        if let Some(Node::Group(group, _)) = self.nodes.get_mut(ino) {
                            group.remove(group_name);
                        }
                    }
                    Ok(())
                } else {
                    Err(Errno::new_not_exist())
                };
            };

            // find next ancestor
            group = if let Some(group_ino) = group.get(*name) {
                let group_ino = group_ino.clone();
                if let Some(Node::Group(group, _)) = self.nodes.get_mut(&group_ino) {
                    inos.push(group_ino);
                    Ok(group)
                } else {
                    Err(Errno::new_not_exist())
                }
            } else {
                    Err(Errno::new_not_exist())
            }?;
        }
    }*/
}