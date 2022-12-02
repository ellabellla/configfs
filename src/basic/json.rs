#![cfg(feature = "json")]

use std::{sync::Arc, time::Duration, io::Write};

use fuse3::{async_trait, Result, Errno};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};
use tokio::sync::RwLock;

use crate::{ComplexConfigHook, EntryType, Configuration};

const CONTAINING_DATA: &'static str = ".^";

impl From<(&str, &Value)> for EntryType {
    fn from(v: (&str, &Value)) -> Self {
        let (k, v) = v;
        if k == CONTAINING_DATA {
            return EntryType::Data
        }
        match v {
            Value::Null => EntryType::Data,
            Value::Bool(_) => EntryType::Data,
            Value::Number(_) => EntryType::Data,
            Value::String(_) => EntryType::Data,
            Value::Array(_) => EntryType::Object,
            Value::Object(_) => EntryType::Object,
        }
    }
}

trait Size {
    fn len(&self, name: &str) -> u64 ;
    fn byte_len(&self) -> u64 ;
}

impl Size for Value {
    fn len(&self, name: &str) -> u64  {
        if name == CONTAINING_DATA {
            return self.byte_len()
        }
        match self {
            Value::Array(arr) => arr.len() as u64 + 1,
            Value::Object(obj) => obj.len() as u64 + 1,
            _ => {
                self.byte_len()
            }
        }
    }
    fn byte_len(&self) -> u64 {
        serde_json::to_string_pretty(&self).unwrap_or("".to_string()).as_bytes().len() as u64
    }
}

#[derive(Debug)]
pub struct JsonConfig {
    root: Value,
    output: Option<String>,
    interval: Duration,
    modified:  bool,
}

impl Serialize for JsonConfig {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.root.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsonConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> 
    {
        let root = Value::deserialize(deserializer)?;
        if !matches!(root, Value::Array(_)) && !matches!(root, Value::Object(_)) {
            Err(serde::de::Error::custom("Root value must be an object or array."))
        } else {
            Ok(JsonConfig{root, output: None, interval: Duration::from_secs(0), modified: false})
        }
    }
}

impl Into<Configuration> for JsonConfig {
    fn into(self) -> Configuration {
        Configuration::Complex(Arc::new(RwLock::new(self)))
    }
}

impl JsonConfig {
    pub fn new_obj(output: Option<(&str, Duration)>) -> Configuration {
        let (output, interval) = if let Some((output, interval)) = output {
            (Some(output.to_string()), interval)
        } else {
            (None, Duration::from_secs(0))
        };
        Configuration::Complex(Arc::new(RwLock::new(JsonConfig{root: Value::Object(Map::new()), output, interval, modified: false})))
    } 

    pub fn new_arr(output: Option<(&str, Duration)>) -> Configuration {
        let (output, interval) = if let Some((output, interval)) = output {
            (Some(output.to_string()), interval)
        } else {
            (None, Duration::from_secs(0))
        };
        Configuration::Complex(Arc::new(RwLock::new(JsonConfig{root: Value::Array(Vec::new()), output, interval, modified: false})))
    } 

    pub fn set_output(&mut self, output: Option<(&str, Duration)>) {
        let (output, interval) = if let Some((output, interval)) = output {
            (Some(output.to_string()), interval)
        } else {
            (None, Duration::from_secs(0))
        };
        self.interval = interval;
        self.output = output;
    }

    fn insert<'a>(parent: &'a mut Value, key: &str, child: Value) -> Result<()> {
        if key == CONTAINING_DATA {
            return Err(libc::EACCES.into());
        }

        match parent {
            Value::Array(arr) => {
                let Ok(index) = usize::from_str_radix(key, 10) else {
                    return Err(libc::EACCES.into())
                };

                if index < arr.len() {
                    arr[index] = child;
                } else if index == arr.len() {
                    arr.push(child);
                } else {
                    return Err(libc::EIO.into())
                }
            },
            Value::Object(obj) => {
                obj.insert(key.to_string(), child);
            },
            _ => return Err(Errno::new_not_exist())
        }
        Ok(())
    }

    fn remove<'a>(key: &str, value: &'a mut Value) -> Result<Value> {
        if key == CONTAINING_DATA {
            return Ok(value.clone());
        }

        Ok(match value {
            Value::Array(arr) => {
                let Ok(index) = usize::from_str_radix(key, 10) else {
                    return Err(Errno::new_not_exist())
                };

                if index >= arr.len() {
                    return Err(Errno::new_not_exist())
                }

                let value = arr.remove(index);

                value
            },
            Value::Object(obj) => {
                let Some(value) = obj.remove(key) else {
                    return Err(Errno::new_not_exist())
                };

                value
            },
            _ => return Err(Errno::new_not_exist())
        })
    }

    fn get<'a>(key: &str, value: &'a Value) -> Result<&'a Value> {
        if key == CONTAINING_DATA {
            return Ok(value);
        }

        Ok(match value {
            Value::Array(arr) => {
                let Ok(index) = usize::from_str_radix(key, 10) else {
                    return Err(Errno::new_not_exist())
                };

                let Some(value) = arr.get(index) else {
                    return Err(Errno::new_not_exist())
                };

                value
            },
            Value::Object(obj) => {
                let Some(value) = obj.get(key) else {
                    return Err(Errno::new_not_exist())
                };

                value
            },
            _ => return Err(Errno::new_not_exist())
        })
    }

    fn get_mut<'a>(key: &str, value: &'a mut Value) -> Result<&'a mut Value> {
        if key == CONTAINING_DATA {
            return Ok(value);
        }

        Ok(match value {
            Value::Array(arr) => {
                let Ok(index) = usize::from_str_radix(key, 10) else {
                    return Err(Errno::new_not_exist())
                };

                let Some(value) = arr.get_mut(index) else {
                    return Err(Errno::new_not_exist())
                };

                value
            },
            Value::Object(obj) => {
                let Some(value) = obj.get_mut(key) else {
                    return Err(Errno::new_not_exist())
                };

                value
            },
            _ => return Err(Errno::new_not_exist())
        })
    }

    fn find_path(&self, path: &Vec<&str>) -> Result<&Value> {
        let mut value = &self.root;
        let mut path = path.iter().peekable();

        while let Some(key) = path.next() {
            if *key == CONTAINING_DATA {
                if matches!(path.peek(), None) {
                    continue;
                } else {
                    return Err(Errno::new_not_exist())
                }
            }

            value = JsonConfig::get(*key, value)?;
        }

        Ok(value)
    }

    fn find_path_mut(&mut self, path: &Vec<&str>) -> Result<&mut Value> {
        let mut value = &mut self.root;
        let mut path = path.iter().peekable();

        while let Some(key) = path.next() {
            if *key == CONTAINING_DATA {
                if matches!(path.peek(), None) {
                    continue;
                } else {
                    return Err(Errno::new_not_exist())
                }
            }

            value = JsonConfig::get_mut(*key, value)?;
        }

        Ok(value)
    }

    fn find(&self, parent: &Vec<&str>, name: &str) -> Result<&Value> {
        let mut value = &self.root;
        let mut ancestors = parent.iter().peekable();

        while let Some(key) = ancestors.next() {
            if *key == CONTAINING_DATA {
                if matches!(ancestors.peek(), None) {
                    continue;
                } else {
                    return Err(Errno::new_not_exist())
                }
            }

            value = JsonConfig::get(*key, value)?;
        }

        if name != CONTAINING_DATA {
            value = JsonConfig::get(name, value)?;
        }

        Ok(value)
    }

    #[allow(dead_code)]
    fn find_mut(&mut self, parent: &Vec<&str>, name: &str) -> Result<&mut Value> {
        let mut value = &mut self.root;
        let mut ancestors = parent.iter().peekable();

        while let Some(key) = ancestors.next() {
            if *key == CONTAINING_DATA {
                if matches!(ancestors.peek(), None) {
                    continue;
                } else {
                    return Err(Errno::new_not_exist())
                }
            }

            value = JsonConfig::get_mut(*key, value)?;
        }

        if name != CONTAINING_DATA {
            value = JsonConfig::get_mut(name, value)?;
        }

        Ok(value)
    }
}

#[async_trait]
impl ComplexConfigHook for JsonConfig {
    async fn entires(&self, parent: &Vec<&str>) -> Result<Vec<String>> {
        let value = self.find_path(parent)?;
        match value {
            Value::Array(arr) => Ok(
                (0..arr.len()).into_iter()
                .map(|i| i.to_string())
                .chain(vec![CONTAINING_DATA.to_string()].into_iter())
                .collect()
            ),
            Value::Object(obj) => Ok(
                obj.keys().into_iter()
                .map(|s| s.to_owned())
                .chain(vec![CONTAINING_DATA.to_string()].into_iter())
                .collect()
        ),
            _ => Err(Errno::new_is_not_dir())
        }
    }
    async fn lookup(&self, parent: &Vec<&str>, name: &str) -> Result<(EntryType, u64)>{
        let value = self.find(parent, name)?;
        Ok((EntryType::from((name, value)), value.len(name)))
    }
    async fn lookup_path(&self, path: &Vec<&str>) -> Result<(EntryType, u64)> {
        let value = self.find_path(path)?;
        let name = path.last().map(|s| *s).unwrap_or("");
        Ok((EntryType::from((name, value)), value.len(name)))
    }
    async fn contains(&self, parent: &Vec<&str>, name: &str) -> bool {
        self.find(parent, name).is_ok()
    }

    async fn mk_data(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>{
        self.modified = true;
        
        let parent = self.find_path_mut(parent)?;
        let Err(_) = JsonConfig::get(name, parent) else {
            return Err(Errno::new_exist())
        };
        JsonConfig::insert(parent, name, Value::Null)
    }
    async fn mk_obj(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>{
        self.modified = true;

        let parent = self.find_path_mut(parent)?;
        let Err(_) = JsonConfig::get(name, parent) else {
            return Err(Errno::new_exist())
        };
        JsonConfig::insert(parent, name, Value::Object(Map::new()))
    }
    async fn mv(&mut self, parent: &Vec<&str>, new_parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>{
        self.modified = true;

        let _ = self.find_path(new_parent)?;
        let parent = self.find_path_mut(parent)?;
        let value = JsonConfig::remove(name, parent)?;

        let new_parent = self.find_path_mut(new_parent)?;
        JsonConfig::insert(new_parent, new_name, value)
    }
    async fn rm(&mut self, parent: &Vec<&str>, name: &str) -> Result<()>{
        self.modified = true;

        let parent = self.find_path_mut(parent)?;
        JsonConfig::remove(name, parent)?;
        Ok(())
    }
    async fn rn(&mut self, parent: &Vec<&str>, name: &str, new_name: &str) -> Result<()>{
        if name == new_name {
            return Ok(())
        }

        self.modified = true;

        let parent = self.find_path_mut(parent)?;
        if let Value::Array(_) = parent {
            let value = JsonConfig::get(name, parent)?;
            JsonConfig::insert(parent, new_name, value.clone())?;
            JsonConfig::remove(name, parent).map(|_| ())
        } else {
            let value = JsonConfig::remove(name, parent)?;
            JsonConfig::insert(parent, new_name, value)
        }
    }

    async fn fetch(&mut self, data_node: &Vec<&str>) -> Result<Vec<u8>>{
        let value = self.find_path(data_node)?;
        Ok(serde_json::to_string_pretty(value).unwrap_or("".to_string()).as_bytes().to_vec())
    }
    async fn size(&mut self, data_node: &Vec<&str>) -> Result<u64>{
        let value = self.find_path(data_node)?;
        Ok(value.byte_len())
    }
    async fn update(&mut self, data_node: &Vec<&str>, data: Vec<u8>) -> Result<()>{
        self.modified = true;

        let value = self.find_path_mut(data_node)?;
        let str = String::from_utf8(data).map_err(|_| Errno::from(libc::EIO))?;
        let new_value: Value = serde_json::from_str(&str).map_err(|_| Errno::from(libc::EIO))?;
        *value = new_value;
        Ok(())
    }

    async fn tick(&mut self) {
        if !self.modified {
            return;
        }

        let value = self.root.clone();
        let output = self.output.clone();
        let Some(output) = output else {return };
        
        let Ok(data) = serde_json::to_string_pretty(&value) else {return};
        if tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut file = std::fs::File::create(output)?;
            file.write_all(data.as_bytes())
        }).await.is_err() {
            println!("failed to output json")
        }
    }

    fn tick_interval(&self) -> Duration {
        self.interval
    }
}

#[cfg(test)]
mod tests {
    use std::{time::Duration, fs};

    use serde_json::{Value, Map};

    use crate::{Configuration, EntryType};

    use super::JsonConfig;

    #[test]
    fn test_serialize() {
        serde_json::from_str::<JsonConfig>("null").unwrap_err();
        serde_json::from_str::<JsonConfig>("10").unwrap_err();
        serde_json::from_str::<JsonConfig>("\"\"").unwrap_err();
        serde_json::from_str::<JsonConfig>("true").unwrap_err();
        let _: JsonConfig = serde_json::from_str("{}").unwrap();
        let _: JsonConfig = serde_json::from_str("[]").unwrap();
        assert_eq!(serde_json::to_string(&JsonConfig{root: Value::Array(Vec::new()), output: None, interval: Duration::from_secs(0), modified: false}).unwrap(), "[]");
        assert_eq!(serde_json::to_string(&JsonConfig{root: Value::Object(Map::new()), output: None, interval: Duration::from_secs(0), modified: false}).unwrap(), "{}");
    }

    #[tokio::test]
    async fn test() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let Configuration::Complex(obj) = JsonConfig::new_obj(Some((
            &tmp.path().to_string_lossy().to_string(), 
            Duration::from_secs(0)
        ))) else {
            unreachable!()
        };
        let mut obj = obj.write().await;
        
        let sort = |mut v:Vec<_>| {v.sort(); v};

        assert_eq!(obj.entires(&vec![]).await.unwrap(), vec![".^"]);

        assert!(obj.entires(&vec![".^", ".^"]).await.is_err());
        assert_eq!(obj.lookup_path(&vec![".^"]).await.unwrap(), (EntryType::Data, "[]".as_bytes().len() as u64));

        obj.mk_data(&vec![], "null").await.unwrap();
        obj.update(&vec!["null"], "null".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.size(&vec!["null"]).await.unwrap(), "null".as_bytes().len() as u64);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["null"]).await.unwrap()).unwrap(), "null");

        obj.mk_data(&vec![], "string").await.unwrap();
        obj.update(&vec!["string"], "\"a string\"".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.size(&vec!["string"]).await.unwrap(), "\"a string\"".as_bytes().len() as u64);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["string"]).await.unwrap()).unwrap(), "\"a string\"");
        
        obj.mk_data(&vec![], "number").await.unwrap();
        obj.update(&vec!["number"], "100".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.size(&vec!["number"]).await.unwrap(), "100".as_bytes().len() as u64);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["number"]).await.unwrap()).unwrap(), "100");
        
        obj.mk_data(&vec![], "bool").await.unwrap();
        obj.update(&vec!["bool"], "true".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.size(&vec!["bool"]).await.unwrap(), "true".as_bytes().len() as u64);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["bool"]).await.unwrap()).unwrap(), "true");

        assert_eq!(obj.lookup(&vec![], "bool").await.unwrap(), (EntryType::Data, "true".as_bytes().len()  as u64));
        assert_eq!(obj.lookup_path(&vec!["bool"]).await.unwrap(), (EntryType::Data, "true".as_bytes().len()  as u64));

        obj.mk_data(&vec![], "arr").await.unwrap();
        obj.update(&vec!["arr"], "[\"one\", \"two\", \"three\"]".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.entires(&vec!["arr"]).await.unwrap(), vec!["0", "1", "2", ".^"]);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "0"]).await.unwrap()).unwrap(), "\"one\"");
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "1"]).await.unwrap()).unwrap(), "\"two\"");
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "2"]).await.unwrap()).unwrap(), "\"three\"");
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", ".^"]).await.unwrap()).unwrap(), "[\n  \"one\",\n  \"two\",\n  \"three\"\n]");

        assert_eq!(obj.lookup(&vec!["arr"], "0").await.unwrap(), (EntryType::Data, "\"one\"".as_bytes().len()  as u64));

        obj.rm(&vec!["arr"], "1").await.unwrap();
        assert_eq!(obj.entires(&vec!["arr"]).await.unwrap(), vec!["0", "1", ".^"]);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "0"]).await.unwrap()).unwrap(), "\"one\"");
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "1"]).await.unwrap()).unwrap(), "\"three\"");

        obj.rn(&vec!["arr"], "1", "0").await.unwrap();
        assert_eq!(obj.entires(&vec!["arr"]).await.unwrap(), vec!["0", ".^"]);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["arr", "0"]).await.unwrap()).unwrap(), "\"three\"");

        obj.mk_data(&vec![], "obj_data").await.unwrap();
        obj.update(&vec!["obj_data"], "{}".as_bytes().to_vec()).await.unwrap();
        assert_eq!(obj.entires(&vec!["obj_data"]).await.unwrap(), vec![".^"]);
        obj.update(&vec!["obj_data", ".^"], "{\"entry\":true}".as_bytes().to_vec()).await.unwrap();
        assert_eq!(sort(obj.entires(&vec!["obj_data"]).await.unwrap()), vec![".^", "entry"]);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["obj_data", "entry"]).await.unwrap()).unwrap(), "true");

        obj.mk_obj(&vec![], "obj").await.unwrap();
        obj.mk_data(&vec!["obj"], "data").await.unwrap();
        obj.mk_obj(&vec!["obj"], "inner").await.unwrap();
        obj.mk_data(&vec!["obj", "inner"], "data").await.unwrap();
        assert_eq!(sort(obj.entires(&vec!["obj"]).await.unwrap()), vec![".^", "data", "inner"]);
        assert_eq!(String::from_utf8(obj.fetch(&vec!["obj", ".^"]).await.unwrap()).unwrap(), "{\n  \"data\": null,\n  \"inner\": {\n    \"data\": null\n  }\n}");

        assert_eq!(obj.lookup(&vec!["obj"], "data").await.unwrap(), (EntryType::Data, "null".as_bytes().len()  as u64));

        obj.rn(&vec!["obj"], "data", "data_new").await.unwrap();
        assert!(obj.contains(&vec!["obj"], "data_new").await);

        obj.mv(&vec!["obj"], &vec!["arr"], "inner", "1").await.unwrap();
        assert_eq!(obj.lookup(&vec!["arr"], "1").await.unwrap(), (EntryType::Object, 2));

        obj.mv(&vec!["arr"], &vec!["obj"], "0", "other data").await.unwrap();
        assert_eq!(obj.lookup(&vec!["obj"], "other data").await.unwrap(), (EntryType::Data, "\"three\"".as_bytes().len() as u64));

        let data = String::from_utf8(obj.fetch(&vec![".^"]).await.unwrap()).unwrap();
        obj.tick().await;
        let file_data = fs::read_to_string(tmp.path()).unwrap();
        assert_eq!(data, file_data);
    }
}