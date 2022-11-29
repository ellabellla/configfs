use std::{sync::Arc, collections::{HashMap}};

use fuse3::{Result, async_trait, Errno};
use tokio::sync::{Mutex, RwLock};
use rand::{Rng};
use crate::{InoLookup, Lookup, Data, Object, EntryType, NodeObject};


pub struct EmptyInoLookup;

impl EmptyInoLookup {
    pub fn new() -> InoLookup {
        Arc::new(RwLock::new(EmptyInoLookup))
    }
}

#[async_trait]
impl Lookup for EmptyInoLookup {
    async fn lookup(&self, _ino: u64) -> bool {
        false
    }
    async fn redirect(&self, ino: u64) -> u64 {
        ino
    }
}


pub struct EmptyNodeObject;

impl EmptyNodeObject {
    pub fn new() -> NodeObject {
        Arc::new(RwLock::new(EmptyNodeObject))
    }
}

#[async_trait]
impl Object for EmptyNodeObject {
    async fn entires(&self, _ino: u64) -> Result<Vec<(u64, EntryType)>>{
        Ok(vec![])
    }
    async fn find(&self, _ino: u64, _path: Vec<String>) -> Result<(u64, EntryType)> {
        Err(Errno::new_not_exist())
    }
    async fn lookup(&self, _ino: u64, _name: String) -> Result<(u64, EntryType)> {
        Err(Errno::new_not_exist())
    }
    async fn contains(&self, _ino: u64, _name: String) -> bool {
        false
    }
    async fn contains_ino(&self, ino: u64) -> bool {
        false
    }

    async fn mk_data(&mut self, _ino: u64, _name: String) -> Result<u64> {
        Err(libc::EROFS.into())
    }
    async fn mk_obj(&mut self, _ino: u64, _name: String) -> Result<u64> {
        Err(libc::EROFS.into())
    }
    async fn mv(&mut self, _parent: u64, _new_parent: u64, _name: String, _new_name: String) -> Result<()> {
        Err(libc::EROFS.into())
    }
    async fn rm(&mut self, _ino: u64, _name: String) -> Result<()> {
        Err(libc::EROFS.into())
    }
    async fn rn(&mut self, _ino: u64, _name: String, _new_name: String) -> Result<()> {
        Err(libc::EROFS.into())
    }
}

#[async_trait]
impl Data for EmptyNodeObject {
    async fn fetch(&mut self, _ino: u64) -> Result<Vec<u8>>{
        Err(Errno::new_not_exist())
    }
    async fn size(&mut self, _ino: u64) -> Result<u64> {
        Err(Errno::new_not_exist())
    }
    async fn update(&mut self, _ino: u64, _data: Vec<u8>) -> Result<()> {
        Err(Errno::new_not_exist())
    }
}

pub struct StorageNodeObject {
    root: u64,
    obj: HashMap<String, u64>,
    nodes: HashMap<u64, String>
}

impl StorageNodeObject {
    pub fn new(root: u64) -> (NodeObject, InoLookup) {
        let object = Arc::new(RwLock::new(StorageNodeObject{root, obj: HashMap::new(), nodes: HashMap::new() }));
        (object.clone(), object)
    }
}

#[async_trait]
impl Object for StorageNodeObject {
    async fn entires(&self, ino: u64) -> Result<Vec<(u64, EntryType)>>{
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }
        Ok(self.obj.values().map(|ino| (*ino, EntryType::Data)).collect())
    }
    async fn find(&self, ino: u64, path: Vec<String>) -> Result<(u64, EntryType)> {
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }
        if path.len() > 1 {
            Err(Errno::new_not_exist())
        } else {
            let Some(name) = path.last() else {
                return Err(Errno::new_not_exist())
            };
            self.obj.get(name).map(|ino| (*ino, EntryType::Data)).ok_or_else(|| Errno::new_not_exist())
        }
    }
    async fn lookup(&self, ino: u64, name: String) -> Result<(u64, EntryType)> {
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }
        self.obj.get(&name).map(|ino| (*ino, EntryType::Data)).ok_or_else(|| Errno::new_not_exist())
    }
    async fn contains(&self, ino: u64, name: String) -> bool {
        if ino != self.root {
            return false;
        }
        self.obj.get(&name).is_some()
    }
    async fn contains_ino(&self, ino: u64) -> bool {
        if ino == self.root {
            return true;
        }
        self.nodes.get(&ino).is_some()
    }

    async fn mk_data(&mut self, ino: u64, name: String) -> Result<u64> {
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }

        if self.obj.contains_key(&name) {
            return Err(Errno::new_exist())
        }


        let mut ino = rand::thread_rng().gen();
        while self.nodes.contains_key(&ino) {
            ino = rand::thread_rng().gen();
        }

        self.nodes.insert(ino, "".to_string());
        self.obj.insert(name, ino);
        Ok(ino)
    }
    async fn mk_obj(&mut self, _ino: u64, _name: String) -> Result<u64> {
        Err(libc::EROFS.into())
    }
    async fn mv(&mut self, _parent: u64, _new_parent: u64, _name: String, _new_name: String) -> Result<()> {
        Err(libc::EROFS.into())
    }
    async fn rm(&mut self, ino: u64, name: String) -> Result<()> {
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }

        if !self.obj.contains_key(&name) {
            return Err(Errno::new_not_exist())
        }

        self.nodes.remove(&ino);
        self.obj.remove(&name);
        
        Ok(())
    }
    async fn rn(&mut self, ino: u64, name: String, new_name: String) -> Result<()> {
        if ino != self.root {
            return Err(Errno::new_not_exist());
        }

        if self.obj.contains_key(&new_name) {
            return Err(Errno::new_exist())
        }

        let ino = self.obj.remove(&name).ok_or(Errno::new_not_exist())?;
        self.obj.insert(new_name, ino);
        
        Ok(())
    }
}

#[async_trait]
impl Data for StorageNodeObject {
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>>{
        self.nodes.get(&ino).map(|data| data.as_bytes().to_vec()).ok_or_else(|| Errno::new_not_exist())
    }
    async fn size(&mut self, ino: u64) -> Result<u64> {
        self.nodes.get(&ino).map(|data| data.len() as u64).ok_or_else(|| Errno::new_not_exist())
    }
    async fn update(&mut self, _ino: u64, _data: Vec<u8>) -> Result<()> {
        Err(Errno::new_not_exist())
    }
}

#[async_trait]
impl Lookup for StorageNodeObject {
    async fn lookup(&self, ino: u64) -> bool {
        self.nodes.contains_key(&ino)
    }
    async fn redirect(&self, ino: u64) -> u64 {
        if self.nodes.contains_key(&ino) {
            self.root
        } else {
            ino
        }
    }
}

pub struct EmptyNodeData;

impl EmptyNodeData {
    pub fn new() -> Arc<Mutex<EmptyNodeData>> {
        Arc::new(Mutex::new(EmptyNodeData))
    }
}

#[async_trait]
impl Data for EmptyNodeData{
    async fn fetch(&mut self, _ino: u64) -> Result<Vec<u8>> {
        Ok(vec![])
    }
    async fn size(&mut self, _ino: u64) -> Result<u64> {
        Ok(0)
    }
    async fn update(&mut self, _ino: u64, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}


pub struct StoredNodeData {
    data: HashMap<u64, Vec<u8>>
}

impl StoredNodeData {
    pub fn new() -> Arc<Mutex<StoredNodeData>> {
        Arc::new(Mutex::new(StoredNodeData{data: HashMap::new()}))
    }
}

#[async_trait]
impl Data for StoredNodeData{
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>> {
        Ok(self.data.get(&ino).map(|d|d.clone()).unwrap_or_else(||{
            self.data.insert(ino, vec![]);
            vec![]
        }))
    }
    async fn size(&mut self, ino: u64) -> Result<u64> {
        Ok(self.data.get(&ino).map(|d|d.len()).unwrap_or(0) as u64)
    }
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()> {
        self.data.insert(ino, data);
        Ok(())
    }
}


pub struct CheckNodeData {
    expected_fetch: (u64, Vec<u8>),
    expected_update: (u64, Vec<u8>)
}

impl CheckNodeData {
    pub fn new() -> Arc<Mutex<CheckNodeData>> {
        Arc::new(Mutex::new(CheckNodeData{expected_fetch: (0, vec![]), expected_update: (0, vec![])}))
    }

    pub fn set_ex_fetch(&mut self, ino: u64, data: Vec<u8>) {
        self.expected_fetch = (ino, data);
    }

    pub fn set_ex_update(&mut self, ino: u64, data: Vec<u8>) {
        self.expected_update = (ino, data);
    }
}

#[async_trait]
impl Data for CheckNodeData{
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>> {
        assert_eq!(self.expected_fetch.0, ino);
        Ok(self.expected_fetch.1.clone())
    }
    async fn size(&mut self, _ino: u64) -> Result<u64> {
        Ok(self.expected_fetch.1.len() as u64)
    }
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()> {
        assert_eq!(self.expected_update.0, ino);
        assert_eq!(self.expected_update.1, data);
        Ok(())
    }
}