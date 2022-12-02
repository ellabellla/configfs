use std::{sync::Arc, time::Duration, hash::Hash};

use fuse3::{async_trait, Result};
use tokio::sync::RwLock;

#[derive(Clone)]
pub enum Configuration {
    Basic(BasicConfiguration),
    Complex(ComplexConfiguration),
}

impl Configuration {
    pub async fn tick_interval(&self) -> Duration {
        match self {
            Configuration::Basic(config) => {
                let config = config.read().await;
                config.tick_interval()
            },
            Configuration::Complex(config) => {
                let config = config.read().await;
                config.tick_interval()
            },
        }
    }
}

impl PartialEq for Configuration {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Configuration::Basic(l0), Configuration::Basic(r0)) => Arc::ptr_eq(&l0, &r0),
            (Configuration::Basic(_), Configuration::Complex(_)) => false,
            (Configuration::Complex(_), Configuration::Basic(_)) => false,
            (Configuration::Complex(l0), Configuration::Complex(r0)) => Arc::ptr_eq(&l0, &r0),
        }
    }
}

impl Eq for Configuration {

}

impl Hash for Configuration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Configuration::Basic(config) => Arc::as_ptr(&config).hash(state),
            Configuration::Complex(config) => Arc::as_ptr(&config).hash(state),
        }
    }
}

impl From<&Configuration> for EntryType {
    fn from(c: &Configuration) -> Self {
        match c {
            Configuration::Basic(_) => EntryType::Data,
            Configuration::Complex(_) => EntryType::Object,
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntryType {
    Object,
    Data,
}

pub type ComplexConfiguration = Arc<RwLock<dyn ComplexConfigHook + Send + Sync>>;

#[async_trait]
pub trait ComplexConfigHook {
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<String>>;
    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)>;
    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)>;
    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool;

    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;
    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;

    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>>;
    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64>;
    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()>;

    async fn tick(&mut self);
    fn tick_interval(&self) -> Duration;
}

pub type BasicConfiguration = Arc<RwLock<dyn BasicConfigHook + Send + Sync>>;
#[async_trait]
pub trait BasicConfigHook {
    async fn fetch(&mut self) -> Result<Vec<u8>>;
    async fn size(&mut self) -> Result<u64>;
    async fn update(&mut self, data: Vec<u8>) -> Result<()>;
    
    async fn tick(&mut self);
    fn tick_interval(&self) -> Duration;
}

