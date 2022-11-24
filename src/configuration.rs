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

    pub fn root(&self) -> u64 {
        self.root
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
                        Node::Data(_, _) => if i == nodes.len() - 1 {
                            return Ok((*ino, node))
                        } else {
                            return Err(Errno::new_not_exist())
                        },
                        Node::Group(new_group, _) => if i == nodes.len() - 1 {
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
        let ino = if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            if let Some(ino) = group.get(&name) {
                let ino = ino.clone();
                Ok(ino)
            } else {
                Err(Errno::new_not_exist())
            }
        } else {
            Err(Errno::new_not_exist())
        }?;
        if let Some(Node::Group(group, _)) = self.nodes.get(&ino) {
            if group.len() != 0 {
                return Err(libc::ENOTEMPTY.into())
            }
        }
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            group.remove(name);
            self.nodes.remove(&ino);
            Ok(())
        } else {
            Err(Errno::new_not_exist())
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use crate::{Configuration, Fetch, Update, Node};

    #[tokio::test]
    async fn root() {
        let config = Configuration::new();

        let mut config = config.write().await;
        
        {
            let root = config.get_root_mut().unwrap();

            root.insert("test".to_string(), 0);
            root.insert("test2".to_string(), 1);
        }

        {
            let root = config.get_root().unwrap();

            assert!(matches!(root.get("test"), Some(0)));
            assert!(matches!(root.get("test2"), Some(1)));
        }

        {
            let root = config.get_root_mut().unwrap();

            assert!(matches!(root.get("test"), Some(0)));
            assert!(matches!(root.get("test2"), Some(1)));
        }
    }

    #[tokio::test]
    async fn find() {
        let config = Configuration::new();
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let fetch: Fetch = Arc::new(|_| Ok(vec![]));
        let update: Update = Arc::new(|_, _| Ok(()));

        let dir1 = config.create_group(root_ino, "dir1").unwrap();

        let file1 = config.create_file(root_ino, "file1", fetch.clone(), update.clone()).unwrap();
        let file2 = config.create_file(dir1, "file2", fetch.clone(), update.clone()).unwrap();

        assert_eq!(config.find("/file1").unwrap().0, file1);
        assert_eq!(config.find("/dir1").unwrap().0, dir1);
        assert_eq!(config.find("/dir1/file2").unwrap().0, file2);
    }

    #[tokio::test]
    async fn fetch_update() {
        let config = Configuration::new();

        let mut config = config.write().await;
        let root_ino = config.root();
        let expected_fetch = Arc::new(Mutex::new(0u64));
        let expected = expected_fetch.clone();
        let fetch: Fetch = Arc::new(move |ino| {
            let expected = expected.lock().unwrap();
            assert!(ino.eq(&expected));
            Ok(vec![])
        });
        let expected_update = Arc::new(Mutex::new((0u64, vec![0u8])));
        let expected = expected_update.clone();
        let update: Update = Arc::new(move |ino, data| {
            let expected = expected.lock().unwrap();
            assert!(expected.0.eq(&ino));
            assert!(expected.1.eq(&data));
            Ok(())
        });

        let file1 = config.create_file(root_ino, "file1", fetch.clone(), update.clone()).unwrap();
        let dir1 = config.create_group(root_ino, "dir1").unwrap();
        let file2 = config.create_file(dir1, "file2", fetch.clone(), update.clone()).unwrap();

        *expected_fetch.lock().unwrap() = file1;
        config.fetch(file1).unwrap();

        *expected_update.lock().unwrap() = (file1, vec![1u8]);
        config.update(file1, vec![1u8]).unwrap();

        *expected_fetch.lock().unwrap() = file2;
        config.fetch(file2).unwrap();

        *expected_update.lock().unwrap() = (file2, vec![2u8]);
        config.update(file2, vec![2u8]).unwrap();
    }

    #[tokio::test]
    async fn contains_get_child() {
        let config = Configuration::new();
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let fetch: Fetch = Arc::new(|_| Ok(vec![]));
        let update: Update = Arc::new(|_, _| Ok(()));

        let dir1 = config.create_group(root_ino, "dir1").unwrap();

        let file1 = config.create_file(root_ino, "file1", fetch.clone(), update.clone()).unwrap();
        let file2 = config.create_file(dir1, "file2", fetch.clone(), update.clone()).unwrap();

        assert_eq!(config.get_child(root_ino, "dir1").unwrap(), dir1);
        assert_eq!(config.get_child(root_ino, "file1").unwrap(), file1);
        assert_eq!(config.get_child(dir1, "file2").unwrap(), file2);

        assert!(config.contains(root_ino, "dir1").unwrap());
        assert!(config.contains(root_ino, "file1").unwrap());
        assert!(config.contains(dir1, "file2").unwrap());
    }

    #[tokio::test]
    async fn mv_rename() {
        let config = Configuration::new();
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let fetch: Fetch = Arc::new(|_| Ok(vec![]));
        let update: Update = Arc::new(|_, _| Ok(()));

        let dir1 = config.create_group(root_ino, "dir1").unwrap();
        let dir2 = config.create_group(root_ino, "dir2").unwrap();

        let _file1 = config.create_file(root_ino, "file1", fetch.clone(), update.clone()).unwrap();
        let _file2 = config.create_file(dir1, "file2", fetch.clone(), update.clone()).unwrap();

        assert!(config.contains(root_ino, "file1").unwrap());
        config.mv(root_ino, dir1, "file1", "file").unwrap();
        assert!(config.contains(dir1, "file").unwrap());

        assert!(config.contains(root_ino, "dir1").unwrap());
        config.rename(root_ino, "dir1", "dir").unwrap();
        assert!(config.contains(root_ino, "dir").unwrap());
        assert!(config.contains(dir1, "file").unwrap());
        assert!(config.contains(dir1, "file2").unwrap());

        assert!(config.contains(root_ino, "dir").unwrap());
        config.mv(root_ino, dir2, "dir", "dir").unwrap();
        assert!(config.contains(dir2, "dir").unwrap());
        assert!(config.contains(dir1, "file").unwrap());
        assert!(config.contains(dir1, "file2").unwrap());
    }


    #[tokio::test]
    async fn create_remove() {
        let config = Configuration::new();

        let mut config = config.write().await;
        let root_ino = config.root();
        let fetch: Fetch = Arc::new(|_| Ok(vec![]));
        let update: Update = Arc::new(|_, _| Ok(()));

        let dir1 = config.create_group(root_ino, "dir1").unwrap();
        let dir2 = config.create_group(root_ino, "dir2").unwrap();

        let file1 = config.create_file(root_ino, "file1", fetch.clone(), update.clone()).unwrap();
        let file2 = config.create_file(dir1, "file2", fetch.clone(), update.clone()).unwrap();
        let file3 = config.create_file(dir2, "file3", fetch.clone(), update.clone()).unwrap();

        let root = config.get_root_mut().unwrap();

        let Some(ino) = root.get("dir1") else {
            panic!("should exist")
        };
        assert_eq!(*ino, dir1);
        
        let Some(ino) = root.get("dir2") else {
            panic!("should exist")
        };
        assert_eq!(*ino, dir2);

        let Some(ino) = root.get("file1") else {
            panic!("should exist")
        };
        assert_eq!(*ino, file1);
        
        let Node::Group(group, parent) = config.fetch(dir1).unwrap() else {
            panic!("should be a group");
        };
        assert_eq!(*parent, root_ino);

        let Some(ino) = group.get("file2") else {
            panic!("should exist")
        };
        assert_eq!(*ino, file2);

        let Node::Group(group, parent) = config.fetch(dir2).unwrap() else {
            panic!("should be a group");
        };
        assert_eq!(*parent, root_ino);

        let Some(ino) = group.get("file3") else {
            panic!("should exist")
        };
        assert_eq!(*ino, file3);

        assert!(config.remove(root_ino, "dir1").is_err());
        assert!(config.remove(dir1, "file2").is_ok());
        assert!(!config.contains(dir1, "file2").unwrap());

        assert!(config.remove(root_ino, "dir1").is_ok());
        assert!(!config.contains(root_ino, "dir1").unwrap());

        
        assert!(config.remove(root_ino, "file1").is_ok());
        assert!(!config.contains(root_ino, "file1").unwrap());
    }

}