use std::{sync::Arc};

use fuse3::{async_trait, Result};
use tokio::sync::RwLock;

#[derive(Clone, Copy)]
pub enum EntryType {
    Object,
    Data,
}

pub type Configuration = Arc<RwLock<dyn ConfigHooks + Send + Sync>>;

#[async_trait]
pub trait ConfigHooks {
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<&str>>;
    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)>;
    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)>;
    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool;

    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;
    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;

    async fn lock(&mut self, data_node: &Vec<&str>) -> Result<()>;
    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>>;
    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64>;
    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()>;
}