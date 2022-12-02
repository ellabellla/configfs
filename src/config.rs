use std::{sync::Arc, time::Duration, hash::Hash};

use fuse3::{async_trait, Result};
use tokio::sync::RwLock;

#[derive(Clone)]
/// # Configuration Interface
pub enum Configuration {
    /// Basic file like interface
    Basic(BasicConfiguration),
    /// Complex directory like interface
    Complex(ComplexConfiguration),
}

impl Configuration {
    /// Returns the tick interval of the config interface
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
/// # Entry Type
/// Defines the type of data at a path.
pub enum EntryType {
    /// Object like data. Gets treated as a directory.
    Object,
    /// Data. Gets treated as a file.
    Data,
}

/// # Complex Configuration Type
/// Complex directory like interface. Places the config interface behind a read write lock to help prevent deadlocking and race conditions.
pub type ComplexConfiguration = Arc<RwLock<dyn ComplexConfigHook + Send + Sync>>;

#[async_trait]
/// # Complex Config Trait
/// A complex configuration mus implement this trait
pub trait ComplexConfigHook {
    /// Return the entires inside a Data Entry at the given path.
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<String>>;
    /// Lookup the type and size of an entry inside a given path
    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)>;
    /// Lookup the type and size of an entry at a given path
    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)>;
    /// Check if an entry contains another
    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool;

    /// Make a data entry at the given path
    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    /// Make an object entry at the given path
    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    /// Move an entry from one parent to another
    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;
    /// Remove an entry
    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>;
    /// Rename an entry
    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>;

    /// Fetch the data of a data entry
    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>>;
    /// Fetch the size of a data entry
    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64>;
    /// Update the data of a data entry
    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()>;

    /// Tick will be called at a given interval allowing the config to periodically perform acts
    async fn tick(&mut self);
    /// Returns the tick interval. This will only be called once.
    fn tick_interval(&self) -> Duration;
}

pub type BasicConfiguration = Arc<RwLock<dyn BasicConfigHook + Send + Sync>>;
#[async_trait]
/// # Basic Config Trait
/// A basic configuration mus implement this trait
pub trait BasicConfigHook {
    /// Fetch the data
    async fn fetch(&mut self) -> Result<Vec<u8>>;
    /// Get the size of the data
    async fn size(&mut self) -> Result<u64>;
    /// Update the data
    async fn update(&mut self, data: Vec<u8>) -> Result<()>;
    
    /// Tick will be called at a given interval allowing the config to periodically perform acts
    async fn tick(&mut self);
    /// Returns the tick interval. This will only be called once.
    fn tick_interval(&self) -> Duration;
}

