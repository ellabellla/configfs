use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::{Node, Configuration, NodeData};

pub trait InoDecoder {
    fn decode(&mut self, ino: u64) -> Option<NodeData>;
}

#[derive(Serialize, Deserialize)]
pub enum  NodeInfo {
    Data,
    Group(HashMap<String, u64>, u64),
}

#[derive(Serialize, Deserialize)]
pub struct  ConfigurationInfo(HashMap<u64, NodeInfo>);

impl ConfigurationInfo {
    pub fn from(config: &Configuration) -> Self {
        let mut nodes = HashMap::with_capacity(config.nodes.len());

        for (ino, node) in config.nodes.iter()
        {
            match node {
                Node::Data(_) => nodes.insert(*ino, NodeInfo::Data),
                Node::Group(group, parent) => nodes.insert(*ino, NodeInfo::Group(group.to_owned(), *parent)),
            };
        }

        ConfigurationInfo(nodes)
    }
}

pub trait FromInfo {
    fn from_info(info: ConfigurationInfo, decoder: &mut Box<dyn InoDecoder>) -> Self;
}

impl FromInfo for Configuration {
    fn from_info(info: ConfigurationInfo, decoder: &mut Box<dyn InoDecoder>) -> Self {
        let mut nodes = HashMap::new();

        for (ino, node) in info.0 {
            match node {
                NodeInfo::Data => decoder.decode(ino)
                    .and_then(|data| nodes.insert(ino, Node::Data(data))),
                NodeInfo::Group(group, parent) => nodes.insert(ino, Node::Group(group, parent)),
            };
        }

        Configuration { nodes }
    }
}


pub struct TestDecode {
    node_data: NodeData,
}

impl TestDecode {
    pub fn new(node_data: NodeData) -> TestDecode {
        TestDecode { node_data }
    }
}

impl<'a> InoDecoder for TestDecode {
    fn decode(&mut self, _ino: u64) -> Option<NodeData> {
        Some(self.node_data.clone())
    }
}


#[cfg(test)]
mod tests {
    use std::{fs::{File}};

    use tempfile::{NamedTempFile};

    use crate::EmptyNodeData;

    use super::*;

    #[tokio::test]
    async fn test() {
        let config = Configuration::new();
        let mut config = config.write().await;
        let root_ino = config.root();
        let node_data = EmptyNodeData::new();

        let _file1 = config.create_file(root_ino, "file1", node_data.clone()).unwrap();
        let dir1 = config.create_group(root_ino, "dir1").unwrap();
        let _file2 = config.create_file(dir1, "file2", node_data.clone()).unwrap();

        let info = config.extract_info();

        let out = NamedTempFile::new().unwrap();
        serde_json::to_writer_pretty(&File::options().write(true).open(out.path()).unwrap(), &info).unwrap();
        let se_info: ConfigurationInfo = serde_json::from_reader(&File::open(out.path()).unwrap()).unwrap();
        out.close().unwrap();

        for (ino, node) in info.0.iter() {
            let Some(se_node) = se_info.0.get(ino) else {
                panic!("node doesn't exist in serialized info")
            };
            assert!(match node {
                NodeInfo::Data => matches!(se_node, NodeInfo::Data),
                NodeInfo::Group(group, parent) => 'group_check: {
                    let NodeInfo::Group(se_group, se_parent) = se_node else {
                        panic!("nodes of the same ino should be the same type")
                    };

                    if *parent != *se_parent {
                        break 'group_check false;
                    }

                    for (name, ino) in group {
                        let Some(se_ino) = se_group.get(name) else {
                            panic!("node doesn't exist in serialized info")
                        };

                        if *ino != *se_ino {
                            break 'group_check false;
                        }
                    }

                    true
                },
            })
        }

        let mut decoder: Box<dyn InoDecoder> = Box::new(TestDecode::new(node_data.clone()));

        let se_config = Configuration::load(se_info, &mut decoder);
        let se_config = se_config.write().await;

        for (ino, node) in config.nodes.iter() {
            let Some(se_node) = se_config.nodes.get(&ino) else {
                panic!("node doesn't exist in serialized info")
            };
            assert!(match node {
                Node::Data(_) => matches!(se_node, Node::Data(_)),
                Node::Group(group, parent) => 'group_check: {
                    let Node::Group(se_group, se_parent) = se_node else {
                        panic!("nodes of the same ino should be the same type")
                    };

                    if *parent != *se_parent {
                        break 'group_check false;
                    }

                    for (name, ino) in group {
                        let Some(se_ino) = se_group.get(name) else {
                            panic!("node doesn't exist in serialized info")
                        };

                        if *ino != *se_ino {
                            break 'group_check false;
                        }
                    }

                    true
                },
            })
        }
    }
}