use std::{env::{self}, time::Duration, collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use configfs::{Mount, FS, basic::json::JsonConfig, Configuration, ComplexConfiguration, ComplexConfigHook, async_trait, Errno, Result, EntryType};
use tokio::{fs::{self}, sync::{RwLock}};

const LOADED_FILE: &str = ".LOADED";

struct LoaderConfig {
    configs: HashMap<String, ComplexConfiguration>,
    loaded: Vec<String>,
    loaded_size: u64,
}

impl LoaderConfig {
    pub fn new() -> Configuration {
        Configuration::Complex(Arc::new(RwLock::new(LoaderConfig{configs: HashMap::new(), loaded: Vec::new(), loaded_size: 0})))
    }

    pub fn find_config<'a>(&self, parent: &'a Vec<&str>) -> Result<(Vec<&'a str>, ComplexConfiguration)> {
        if parent.len() == 0{
            return Err(Errno::new_not_exist())
        }

        let mut parent = parent.clone();
        let key = parent.remove(0);


        self.configs.get(key)
            .map(|c| (parent, c.clone()))
            .ok_or_else(||Errno::new_not_exist())
    }

    pub async fn update_loaded_configs(&mut self, data: Vec<u8>) -> Result<()> {
        let config_paths = String::from_utf8(data).map_err(|_| Errno::from(libc::EIO))?;
        self.loaded_size = config_paths.as_bytes().len() as u64;
        let config_paths = config_paths.split("\n").filter(|s| *s != "");

        let mut configs = HashMap::new();
        let mut loaded = Vec::new();
        for path in config_paths
        {
            let name =  PathBuf::from_str(path)
                .map_err(|_| Errno::from(libc::EACCES))?
                .file_name()
                .ok_or_else(||Errno::from(libc::EACCES))?
                .to_string_lossy()
                .to_string();
            if name == LOADED_FILE {
                return Err(libc::EACCES.into())
            }
            
            let path = path.to_string();
            
            let json = fs::read_to_string(&path).await?;
            let mut json_config: JsonConfig = serde_json::from_str(&json).map_err(|_|Errno::from(libc::EACCES))?;
            json_config.set_output(Some((&path, Duration::from_secs(1))));

            let complex: Arc<RwLock<dyn ComplexConfigHook + Send + Sync>> = Arc::new(RwLock::new(json_config));

            let mut i = 0;
            let mut config_name = name.to_string();
            while configs.contains_key(&config_name) {
                config_name = format!("{}_{}", name, i);
                i += 1;
            }
            
            configs.insert(config_name, complex);
            loaded.push(path)
        }
        self.configs = configs;
        self.loaded = loaded;
        return Ok(())
    }

    pub fn fetch_loaded_configs(&self) -> Vec<u8> {
        self.loaded.iter().map(|s| s.to_string()).collect::<Vec<String>>().join("\n").as_bytes().to_vec()
    }
}

#[async_trait]
impl ComplexConfigHook for LoaderConfig {
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<String>> {
        if parent.len() == 0 {
            Ok(self.configs.keys().map(|s| s.to_string()).chain(vec![LOADED_FILE.to_string()].into_iter()).collect())
        } else {
            let (parent, config) = self.find_config(parent)?;
            let config = config.read().await;

            config.entires(&parent).await
        }
    }

    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)> {
        if parent.len() == 0 && name == LOADED_FILE {
            return Ok((EntryType::Data, self.loaded_size))
        } else  if parent.len() == 0 {
            let Some(config) = self.configs.get(name) else {
                return Err(Errno::new_not_exist())
            };

            config.read().await.lookup_path(parent).await
        } else {
            let (parent, config) = self.find_config(parent)?;
            let config = config.read().await;

            config.lookup(&parent, name).await
        }
    }

    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)> {
        if path.len() == 0 {
            return Ok((EntryType::Object, self.configs.len() as u64 + 1));
        } else if path.len() == 1 && path[0] == LOADED_FILE {
            return Ok((EntryType::Data, self.loaded_size))
        } else {
            let (path, config) = self.find_config(path)?;
            let config = config.read().await;

            config.lookup_path(&path).await
        }
    }

    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool {
        return (parent.len() == 0 && name == LOADED_FILE) 
            || self.find_config(parent).is_ok()
    }

    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        if parent.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let (parent, config) = self.find_config(parent)?;
            let mut config = config.write().await;

            config.mk_data(&parent, name).await
        }
    }

    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        if parent.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let (parent, config) = self.find_config(parent)?;
            let mut config = config.write().await;

            config.mk_obj(&parent, name).await
        }
    }

    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()> {
        if parent.len() == 0 || new_parent.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let (parent, config) = self.find_config(parent)?;

            let (new_parent, new_config) = self.find_config(new_parent)?;

            if !Arc::ptr_eq(&config, &new_config) {
                return Err(libc::EACCES.into())
            }

            let mut config = config.write().await;

            config.mv(&parent, &new_parent, name, new_name).await
        }
    }

    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()> {
        if parent.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let (parent, config) = self.find_config(parent)?;
            let mut config = config.write().await;

            config.rm(&parent, name).await
        }
    }

    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()> {
        if parent.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let (parent, config) = self.find_config(parent)?;
            let mut config = config.write().await;

            config.rn(&parent, name, new_name).await
        }
    }

    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>> {
        if data_node.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let mut data_node = data_node.clone();
            let key = data_node.remove(0);

            if key == LOADED_FILE && data_node.len() == 0 {
                return Ok(self.fetch_loaded_configs())
            }

            let config = self.configs.get(key);
            let Some(config) =  config else {
                return Err(Errno::new_not_exist());
            };

            config.write().await.fetch(&data_node).await
        }
    }

    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64> {
        if data_node.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let mut data_node = data_node.clone();
            let key = data_node.remove(0);
            
            if key == LOADED_FILE {
                return Ok(self.loaded_size)
            }

            let config = self.configs.get(key);
            let Some(config) =  config else {
                return Err(Errno::new_not_exist());
            };

            config.write().await.size(&data_node).await
        }
    }

    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()> {
        if data_node.len() == 0 {
            Err(libc::EACCES.into())
        } else {
            let mut data_node = data_node.clone();
            let key = data_node.remove(0);
            
            if key == LOADED_FILE {
                return self.update_loaded_configs(data).await
            }

            let config = self.configs.get(key);
            let Some(config) =  config else {
                return Err(Errno::new_not_exist());
            };

            config.write().await.update(&data_node, data).await
        }
    }

    async fn tick(&mut self) {
        for config in self.configs.values() {
            config.write().await.tick().await;
        }
    }
    fn tick_interval(&self) -> Duration {
        Duration::from_secs(1)
    }
}

#[tokio::main]
pub async fn main() {
    let mut args = env::args().skip(1);

    let Some(mnt) = args.next() else {
        println!("a mount point must be specified");
        return;
    };

    let mount = Mount::new();
    {
        let mut mnt = mount.write().await;
        mnt.mount("/", LoaderConfig::new());
    }
    FS::mount("test", &mnt, mount)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap()
} 