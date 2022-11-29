use std::{sync::Arc, collections::HashMap};

use fuse3::{Errno, Result};
use rand::Rng;
use tokio::sync::RwLock;

use crate::{Node, NodeData, serde::{ConfigurationInfo, FromInfo, InoDecoder}, NodeObject, InoLookup, basic::EmptyInoLookup};

pub struct Configuration {
    pub(crate) nodes: HashMap<u64, Node>,
    pub(crate) ino_lookup: InoLookup,
}

pub const ROOT: u64 = 1;

impl Configuration {
    pub fn new(ino_lookup: InoLookup) -> Arc<RwLock<Configuration>> {
        let mut nodes = HashMap::new();
        nodes.insert(ROOT, Node::Group(HashMap::new(), 0));
        Arc::new(RwLock::new(Configuration{nodes, ino_lookup}))
    }

    pub fn load(info: ConfigurationInfo, decoder: &mut Box<dyn InoDecoder>) -> Arc<RwLock<Configuration>> {
        Arc::new(RwLock::new(Configuration::from_info(info, decoder, EmptyInoLookup::new())))
    }

    pub fn extract_info(&self) -> ConfigurationInfo {
        ConfigurationInfo::from(self)
    }

    fn split_path(path: &str) -> Vec<&str> {
        path.split('/').filter(|s| *s != "").collect()
    }

    async fn new_ino(&mut self, node: Node) -> u64 {
        let mut ino = rand::thread_rng().gen();
        let ino_lookup = self.ino_lookup.read().await;
        while self.nodes.contains_key(&ino) || ino_lookup.lookup(ino).await {
            ino = rand::thread_rng().gen();
        }

        self.nodes.insert(ino, node);
        ino
    }

    async fn add_ino(&mut self, ino: u64, node: Node) -> Option<u64> {
        let ino_lookup = self.ino_lookup.read().await;
        if self.nodes.contains_key(&ino) || ino_lookup.lookup(ino).await {
            None
        } else {
            self.nodes.insert(ino, node);
            Some(ino)
        }
    }

    pub fn root(&self) -> u64 {
        ROOT
    }

    pub async fn get_root<'a>(&'a self) -> Result<&'a HashMap<String, u64>> {
        if let Node::Group(root, _)  = self.get(ROOT).await? {
            Ok(root)
        } else {
            Err(Errno::new_not_exist())
        }
    }

    pub async fn find(&self, path: &str) -> Result<(u64, &Node)> {
        let nodes = Configuration::split_path(path);

        let mut group = self.get_root().await?;
        for (i, name) in nodes.iter().enumerate() {
            if let Some(ino) = group.get(*name) {
                match self.get(*ino).await? {
                    node => match node {
                        Node::Data(_) => if i == nodes.len() - 1 {
                            return Ok((*ino, node))
                        } else {
                            return Err(Errno::new_not_exist())
                        },
                        Node::Object(obj) => {
                            let (ino, _) = obj.read().await.find(
                                *ino, 
                                nodes.iter()
                                    .map(|s| s.to_string()).collect()
                            ).await?;
                            return Ok((ino, node))
                        },
                        Node::Group(new_group, _) => if i == nodes.len() - 1 {
                            return Ok((*ino, node))
                        } else {
                            group = new_group
                        },
                    },
                }
            } else {
                return Err(Errno::new_not_exist())
            }
        }
        return Err(Errno::new_not_exist())
    }

    pub async fn get<'a>(&'a self, ino: u64) -> Result<&'a Node> {
        let re_ino = self.ino_lookup.read().await.redirect(ino).await;
        if re_ino == ino {
            self.nodes.get(&ino).ok_or_else(|| Errno::new_not_exist())
        } else {
            let Some(node) = self.nodes.get(&re_ino) else {
                return Err(Errno::new_not_exist())
            };
            let Node::Object(obj) = node else {
                return Err(Errno::new_not_exist())
            };

            if !obj.read().await.contains_ino(ino).await {
                return Err(Errno::new_not_exist())
            }

            Ok(node)
        }
    }

    pub async fn fetch_data<'a>(&'a self, ino: u64) -> Result<Vec<u8>> {
        match self.get(ino).await? {
            Node::Data(data) => Ok(data.lock().await.fetch(ino).await?),
            Node::Object(object) => Ok(object.write().await.fetch(ino).await?),
            _ => Err(Errno::new_is_dir()),
        }
    }

    pub async fn update_data(&self, ino: u64, data: Vec<u8>) -> Result<()> {
        match self.get(ino).await? {
            Node::Data(node_data) => Ok(node_data.lock().await.update(ino, data).await?),
            Node::Object(object) => Ok(object.write().await.update(ino, data).await?),
            _ => Err(Errno::new_is_dir()),
        }
    }

    pub async fn contains(&self, ino: u64, name: &str) -> Result<bool> {
        let group = self.get(ino).await?;
        match group {
            Node::Data(_) => Err(Errno::new_not_exist()),
            Node::Group(group, _) => Ok(group.contains_key(name)),
            Node::Object(obj) => Ok(obj.read().await
                .contains(ino, name.to_string()).await),
        }
    }

    pub async fn get_child(&self, ino: u64, name: &str) -> Result<u64> {
        let group = self.get(ino).await?;
        match group {
            Node::Group(group, _) => group.get(name)
                .map(|i| i.clone())
                .ok_or_else(|| Errno::new_not_exist()),
            Node::Object(obj) => obj.read().await
                .lookup(ino, name.to_string()).await
                .map(|(i,_)| i),
            _ =>  Err(Errno::new_not_exist()),
        }
    }
    
    pub async fn mv(&mut self, parent: u64, new_parent: u64, name: &str, new_name: &str) -> Result<()> {
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            let name = name.to_string();
            let Some(ino) = group.get(&name) else {
                return Err(Errno::new_not_exist())
            };
            let ino = ino.clone();
            group.remove(&name);
            let Some(Node::Group(group, _)) = self.nodes.get_mut(&new_parent) else {
                return Err(Errno::new_not_exist())
            };
            if let Some(ino) = group.insert(new_name.to_string(), ino) {
                self.nodes.remove(&ino);
            }
            Ok(())
        } else {
            let Node::Object(obj) = self.get(parent).await? else {
                return Err(Errno::new_is_not_dir())
            };

            obj.write().await.mv(parent, new_parent, name.to_string(), new_name.to_string()).await
        }
    }

    pub async fn rename(&mut self, parent: u64, name: &str, new_name: &str) -> Result<()> {
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
            let Node::Object(obj) = self.get(parent).await? else {
                return Err(Errno::new_is_not_dir())
            };

            obj.write().await.rn(parent, name.to_string(), new_name.to_string()).await
        }
    }

    pub async fn create_group(&mut self, parent: u64, name: &str) -> Result<u64> {
        let ino = self.new_ino(Node::Group(HashMap::new(), parent)).await;
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

    pub async fn create_data(&mut self, parent: u64, name: &str, node_data: NodeData) -> Result<u64> {
        let ino = self.new_ino(Node::Data(node_data)).await;
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

            let Node::Object(obj) = self.get(parent).await? else {
                return Err(Errno::new_is_not_dir())
            };

            obj.write().await.mk_data(parent, name.to_string()).await
        }
    }

    pub async fn create_object(&mut self, parent: u64, name: &str, ino: u64, node_object: NodeObject) -> Result<u64> {
        self.add_ino(ino, Node::Object(node_object)).await.ok_or_else(|| Errno::new_exist())?;
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

            let Node::Object(obj) = self.get(parent).await? else {
                return Err(Errno::new_is_not_dir())
            };

            obj.write().await.mk_obj(parent, name.to_string()).await
        }
    }

    pub async fn remove(&mut self, parent: u64, name: &str) -> Result<()> {
        let ino  = match self.get(parent).await?  {
            Node::Group(group, _) => {
                let name = name.to_string();
                if let Some(ino) = group.get(&name) {
                    let ino = ino.clone();
                    Ok(ino)
                } else {
                    Err(Errno::new_not_exist())
                }
            },
            Node::Object(obj) => obj.read().await
                .lookup(parent, name.to_string()).await
                .map(|(i,_)| i),
            _ => Err(Errno::new_is_not_dir())
        }?;
        if let Node::Group(group, _) = self.get(ino).await? {
            if group.len() != 0 {
                return Err(libc::ENOTEMPTY.into())
            }
        }
        if let Some(Node::Group(group, _)) = self.nodes.get_mut(&parent) {
            group.remove(name);
            self.nodes.remove(&ino);
            Ok(())
        } else {
            let Node::Object(obj) = self.get(parent).await? else {
                return Err(Errno::new_is_not_dir())
            };

            obj.write().await.rm(parent, name.to_string()).await
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::{Configuration, Node, basic::{EmptyNodeData, CheckNodeData, EmptyInoLookup}};

    #[tokio::test]
    async fn root() {
        let config = Configuration::new(EmptyInoLookup::new());

        let mut config = config.write().await;
        let root_id = config.root();

        {
            let Node::Group(root, _)  = config.nodes.get_mut(&root_id).unwrap() else {
                panic!("root should be a group")
            };

            root.insert("test".to_string(), 0);
            root.insert("test2".to_string(), 1);
        }

        {
            let root = config.get_root().await.unwrap();

            assert!(matches!(root.get("test"), Some(0)));
            assert!(matches!(root.get("test2"), Some(1)));
        }

        {
            let Node::Group(root, _)  = config.nodes.get_mut(&root_id).unwrap() else {
                panic!("root should be a group")
            };

            assert!(matches!(root.get("test"), Some(0)));
            assert!(matches!(root.get("test2"), Some(1)));
        }
    }

    #[tokio::test]
    async fn find_get() {
        let config = Configuration::new(EmptyInoLookup::new());
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = EmptyNodeData::new();


        let dir1 = config.create_group(root_ino, "dir1").await.unwrap();

        let file1 = config.create_data(root_ino, "file1", node_data.clone()).await.unwrap();
        let file2 = config.create_data(dir1, "file2", node_data.clone()).await.unwrap();

        assert_eq!(config.find("/file1").await.unwrap().0, file1);
        assert_eq!(config.find("/dir1").await.unwrap().0, dir1);
        assert_eq!(config.find("/dir1/file2").await.unwrap().0, file2);

        assert!(matches!(config.get(dir1).await.unwrap(), Node::Group(_, _)));
        assert!(matches!(config.get(file1).await.unwrap(), Node::Data(_)));
        assert!(matches!(config.get(file2).await.unwrap(), Node::Data(_)));
    }

    #[tokio::test]
    async fn fetch_update() {
        let config = Configuration::new(EmptyInoLookup::new());

        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = CheckNodeData::new();

        let file1 = config.create_data(root_ino, "file1", node_data.clone()).await.unwrap();
        let dir1 = config.create_group(root_ino, "dir1").await.unwrap();
        let file2 = config.create_data(dir1, "file2", node_data.clone()).await.unwrap();

        node_data.lock().await.set_ex_fetch(file1, vec![3u8]);
        assert_eq!(config.fetch_data(file1).await.unwrap(), vec![3u8]);

        node_data.lock().await.set_ex_update(file1, vec![1u8]);
        config.update_data(file1, vec![1u8]).await.unwrap();

        node_data.lock().await.set_ex_fetch(file2, vec![4u8]);
        assert_eq!(config.fetch_data(file2).await.unwrap(), vec![4u8]);

        node_data.lock().await.set_ex_update(file2, vec![2u8]);
        config.update_data(file2, vec![2u8]).await.unwrap();
    }

    #[tokio::test]
    async fn contains_get_child() {
        let config = Configuration::new(EmptyInoLookup::new());
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = EmptyNodeData::new();

        let file1 = config.create_data(root_ino, "file1", node_data.clone()).await.unwrap();
        let dir1 = config.create_group(root_ino, "dir1").await.unwrap();
        let file2 = config.create_data(dir1, "file2", node_data.clone()).await.unwrap();

        assert_eq!(config.get_child(root_ino, "dir1").await.unwrap(), dir1);
        assert_eq!(config.get_child(root_ino, "file1").await.unwrap(), file1);
        assert_eq!(config.get_child(dir1, "file2").await.unwrap(), file2);

        assert!(config.contains(root_ino, "dir1").await.unwrap());
        assert!(config.contains(root_ino, "file1").await.unwrap());
        assert!(config.contains(dir1, "file2").await.unwrap());
    }

    #[tokio::test]
    async fn mv_rename() {
        let config = Configuration::new(EmptyInoLookup::new());
        
        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = EmptyNodeData::new();

        let dir1 = config.create_group(root_ino, "dir1").await.unwrap();
        let dir2 = config.create_group(root_ino, "dir2").await.unwrap();

        let _file1 = config.create_data(root_ino, "file1", node_data.clone()).await.unwrap();
        let _file2 = config.create_data(dir1, "file2", node_data.clone()).await.unwrap();

        assert!(config.contains(root_ino, "file1").await.unwrap());
        config.mv(root_ino, dir1, "file1", "file").await.unwrap();
        assert!(config.contains(dir1, "file").await.unwrap());

        assert!(config.contains(root_ino, "dir1").await.unwrap());
        config.rename(root_ino, "dir1", "dir").await.unwrap();
        assert!(config.contains(root_ino, "dir").await.unwrap());
        assert!(config.contains(dir1, "file").await.unwrap());
        assert!(config.contains(dir1, "file2").await.unwrap());

        assert!(config.contains(root_ino, "dir").await.unwrap());
        config.mv(root_ino, dir2, "dir", "dir").await.unwrap();
        assert!(config.contains(dir2, "dir").await.unwrap());
        assert!(config.contains(dir1, "file").await.unwrap());
        assert!(config.contains(dir1, "file2").await.unwrap());
    }


    #[tokio::test]
    async fn create_remove() {
        let config = Configuration::new(EmptyInoLookup::new());

        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = EmptyNodeData::new();

        let dir1 = config.create_group(root_ino, "dir1").await.unwrap();
        let dir2 = config.create_group(root_ino, "dir2").await.unwrap();

        let file1 = config.create_data(root_ino, "file1", node_data.clone()).await.unwrap();
        let file2 = config.create_data(dir1, "file2", node_data.clone()).await.unwrap();
        let file3 = config.create_data(dir2, "file3", node_data.clone()).await.unwrap();

        let root = config.get_root().await.unwrap();

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
        
        let Node::Group(group, parent) = config.get(dir1).await.unwrap() else {
            panic!("should be a group");
        };
        assert_eq!(*parent, root_ino);

        let Some(ino) = group.get("file2") else {
            panic!("should exist")
        };
        assert_eq!(*ino, file2);

        let Node::Group(group, parent) = config.get(dir2).await.unwrap() else {
            panic!("should be a group");
        };
        assert_eq!(*parent, root_ino);

        let Some(ino) = group.get("file3") else {
            panic!("should exist")
        };
        assert_eq!(*ino, file3);

        assert!(config.remove(root_ino, "dir1").await.is_err());
        assert!(config.remove(dir1, "file2").await.is_ok());
        assert!(!config.contains(dir1, "file2").await.unwrap());

        assert!(config.remove(root_ino, "dir1").await.is_ok());
        assert!(!config.contains(root_ino, "dir1").await.unwrap());

        
        assert!(config.remove(root_ino, "file1").await.is_ok());
        assert!(!config.contains(root_ino, "file1").await.unwrap());
    }

}