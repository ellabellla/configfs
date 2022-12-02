use std::{sync::Arc, collections::HashMap, time::Duration};

use fuse3::{async_trait, Result, Errno};
use tokio::sync::RwLock;

use crate::{EntryType, Configuration, ComplexConfigHook, BasicConfigHook};

enum MemNode {
    Dir(Arc<RwLock<HashMap<String, MemNode>>>),
    File(Arc<Vec<u8>>),
}

impl MemNode {
    async fn len(&self) -> u64 {
        match self {
            MemNode::Dir(dir) => {
                let dir = dir.read().await;
                dir.len() as u64
            },
            MemNode::File(file) => {
                file.len() as u64
            },
        }
    }

    fn is_file(&self) -> bool {
        matches!(self, MemNode::File(_))
    }

    #[allow(dead_code)]
    fn is_dir(&self) -> bool {
        matches!(self, MemNode::Dir(_))
    }
}

impl From<&MemNode> for EntryType {
    fn from(node: &MemNode) -> Self {
        match node {
            MemNode::Dir(_) => EntryType::Object,
            MemNode::File(_) => EntryType::Data,
        }
    }
}

pub struct MemDir {
    tree: Arc<RwLock<HashMap<String, MemNode>>>
}

impl MemDir {
    pub fn new() -> Configuration {
        Configuration::Complex(Arc::new(RwLock::new(MemDir{tree: Arc::new(RwLock::new(HashMap::new()))})))
    }
}

#[async_trait]
impl ComplexConfigHook for MemDir {
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<String>> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter().peekable();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                if ancestors.peek().is_none() {
                    let MemNode::Dir(dir) = node else {
                        return Err(Errno::new_is_not_dir())
                    };

                    let dir = dir.clone();
                    let dir = dir.read().await;
                    return Ok(dir.keys().map(|s| s.to_string()).collect())
                } else {
                    match node {
                        MemNode::Dir(new_dir) => {
                            let new_dir = new_dir.clone();
                            drop(read_dir);
                            dir = new_dir;
                        },
                        MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                    }
                }
            } else {
                let dir = dir.read().await;
                return Ok(dir.keys().map(|s| s.to_string()).collect())
            }
        }
    }

    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let dir = dir.read().await;
        let Some(node) = dir.get(name) else {
            return Err(Errno::new_not_exist())
        };
        return Ok((EntryType::from(node), node.len().await))
    }

    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)> {
        let mut dir = self.tree.clone();
        let mut ancestors = path.iter().peekable();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                if ancestors.peek().is_none() {
                    return Ok((EntryType::from(node), node.len().await))
                } else {
                    match node {
                        MemNode::Dir(new_dir) => {
                            let new_dir = new_dir.clone();
                            drop(read_dir);
                            dir = new_dir;
                        },
                        MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                    }
                }
            } else {
                let dir = dir.read().await;
                return Ok((EntryType::Object, dir.len() as u64))
            }
        }
    }

    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return false
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return false,
                }
            } else {
                break;
            }
        }
        let dir = dir.read().await;
        return dir.contains_key(name)
    }

    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        dir.insert(name.to_string(), MemNode::File(Arc::new(vec![])));
        Ok(())
    }

    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        if dir.contains_key(name) {
            return Err(Errno::new_exist())
        }
        dir.insert(name.to_string(), MemNode::Dir(Arc::new(RwLock::new(HashMap::new()))));
        Ok(())
    }

    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        let Some(node) = dir.remove(name) else {
            return Err(Errno::new_not_exist())
        };
        drop(dir);
        let mut dir = self.tree.clone();
        let mut ancestors = new_parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        dir.insert(new_name.to_string(), node);
        Ok(())
    }

    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        let Some(_) = dir.remove(name) else {
            return Err(Errno::new_not_exist())
        };

        Ok(())
    }

    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = parent.iter();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                match node {
                    MemNode::Dir(new_dir) => {
                        let new_dir = new_dir.clone();
                        drop(read_dir);
                        dir = new_dir;
                    },
                    MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                }
            } else {
                break;
            }
        }
        let mut dir = dir.write().await;
        if dir.contains_key(new_name) {
            return Err(Errno::new_exist())
        }
        let Some(node) = dir.remove(name) else {
            return Err(Errno::new_not_exist())
        };
        dir.insert(new_name.to_string(), node);
        Ok(())
    }

    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>> {
        let mut dir = self.tree.clone();
        let mut ancestors = data_node.iter().peekable();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                if ancestors.peek().is_none() {
                    let MemNode::File(file) = node else {
                        return Err(Errno::new_is_dir())
                    };

                    return Ok(file.to_vec());
                } else {
                    match node {
                        MemNode::Dir(new_dir) => {
                            let new_dir = new_dir.clone();
                            drop(read_dir);
                            dir = new_dir;
                        },
                        MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                    }
                }
            } else {
                return Err(Errno::new_not_exist())
            }
        }
    }

    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64> {
        let mut dir = self.tree.clone();
        let mut ancestors = data_node.iter().peekable();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                if ancestors.peek().is_none() {
                    if node.is_file() {
                        return Ok(node.len().await)
                    } else {
                        return Err(Errno::new_is_dir())
                    }
                } else {
                    match node {
                        MemNode::Dir(new_dir) => {
                            let new_dir = new_dir.clone();
                            drop(read_dir);
                            dir = new_dir;
                        },
                        MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                    }
                }
            } else {
                return Err(Errno::new_not_exist())
            }
        }
    }

    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()> {
        let mut dir = self.tree.clone();
        let mut ancestors = data_node.iter().peekable();
        loop {
            if let Some(ancestor) = ancestors.next() {
                let read_dir = dir.read().await;
                let Some(node) = read_dir.get(*ancestor) else {
                    return Err(Errno::new_not_exist())
                };

                if ancestors.peek().is_none() {
                    let MemNode::File(_) = node else {
                        return Err(Errno::new_is_dir())
                    };

                    drop(read_dir);
                    let mut dir = dir.write().await;
                    dir.insert(ancestor.to_string(), MemNode::File(Arc::new(data)));
                    return Ok(())
                } else {
                    match node {
                        MemNode::Dir(new_dir) => {
                            let new_dir = new_dir.clone();
                            drop(read_dir);
                            dir = new_dir;
                        },
                        MemNode::File(_) => return Err(Errno::new_is_not_dir()),
                    }
                }
            } else {
                return Err(Errno::new_not_exist())
            }
        }
    }

    async fn tick(&mut self) {
            
    }

    fn tick_interval(&self) -> Duration {
        Duration::from_secs(0)
    }
}

pub struct MemFile {
    data: Vec<u8>
}

impl MemFile {
    pub fn new() -> Configuration {
        Configuration::Basic(Arc::new(RwLock::new(MemFile{data: vec![]})))
    }
}

#[async_trait]
impl BasicConfigHook for MemFile {
    async fn fetch(&mut self) -> Result<Vec<u8>> { 
        Ok(self.data.clone())
    }
    async fn size(&mut self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }
    async fn update(&mut self, data: Vec<u8>) -> Result<()> {
        self.data = data;
        Ok(())
    }
    
    async fn tick(&mut self) {
        
    }
    fn tick_interval(&self) -> Duration {
        Duration::from_secs(0)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EntryType;

    use super::{MemDir, Configuration, MemFile};

    #[tokio::test]
    async fn test_memdir() {
        let dir = MemDir::new();
        let Configuration::Complex(dir) = dir else {
            unreachable!()
        };

        let mut dir = dir.write().await;

        let sort = |mut v: Vec<_>| {v.sort(); v};

        dir.mk_data(&vec![], "file").await.unwrap();
        dir.mk_obj(&vec![], "dir").await.unwrap();
        dir.mk_data(&vec!["dir"], "file").await.unwrap();

        assert_eq!(dir.lookup(&vec![], "file").await.unwrap(), (EntryType::Data, 0));
        assert_eq!(dir.lookup(&vec![], "dir").await.unwrap(), (EntryType::Object, 1));
        assert_eq!(dir.lookup(&vec!["dir"], "file").await.unwrap(), (EntryType::Data, 0));

        assert_eq!(dir.lookup_path(&vec!["dir" ]).await.unwrap(), (EntryType::Object, 1));
        assert_eq!(dir.lookup_path(&vec!["dir", "file"]).await.unwrap(), (EntryType::Data, 0));

        assert_eq!(dir.contains(&vec!["dir"],  "file").await, true);

        assert_eq!(sort(dir.entires(&vec![]).await.unwrap()), vec!["dir".to_string(), "file".to_string()]);
        assert_eq!(sort(dir.entires(&vec!["dir"]).await.unwrap()), vec!["file".to_string()]);

        assert_eq!(dir.fetch(&vec!["file"]).await.unwrap(), Vec::<u8>::new());
        dir.update(&vec!["file"], vec![0x1]).await.unwrap();
        assert_eq!(dir.fetch(&vec!["file"]).await.unwrap(), vec![0x1]);
        assert_eq!(dir.size(&vec!["file"]).await.unwrap(), 1);

        dir.mv(&vec!["dir"], &vec![], "file", "file2").await.unwrap();
        assert_eq!(dir.contains(&vec!["dir"],  "file").await, false);
        assert_eq!(dir.contains(&vec![],  "file2").await, true);
        assert_eq!(sort(dir.entires(&vec![]).await.unwrap()), vec!["dir".to_string(), "file".to_string(), "file2".to_string()]);

        dir.rn(&vec![], "file2", "another file").await.unwrap();
        assert_eq!(dir.contains(&vec![],  "file2").await, false);
        assert_eq!(dir.contains(&vec![],  "another file").await, true);

        dir.rm(&vec![], "another file").await.unwrap();
        assert_eq!(dir.contains(&vec![],  "another file").await, false);
    }

    #[tokio::test]
    async fn test_memfile() {
        let file = MemFile::new();
        let Configuration::Basic(file) = file else {
            unreachable!()
        };
        let mut file = file.write().await;

        assert_eq!(file.fetch().await.unwrap(), Vec::<u8>::new());
        assert_eq!(file.size().await.unwrap(), 0);
        file.update(vec![0x1]).await.unwrap();
        assert_eq!(file.fetch().await.unwrap(), vec![0x1]);
        assert_eq!(file.size().await.unwrap(), 1);
    }
}