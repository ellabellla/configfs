use std::{collections::HashMap, sync::Arc, time::{Duration, UNIX_EPOCH}, io};

use fuse3::{Result, raw::{reply::FileAttr, Session}, FileType, MountOptions, async_trait};
use rand::Rng;
use tokio::{sync::{mpsc::{Sender, UnboundedReceiver, self, UnboundedSender}, RwLock, Mutex}, task::JoinHandle};

mod configuration;
mod fs;
pub use configuration::Configuration;

#[async_trait]
pub trait Data {
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>>;
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()>;
}

type NodeData = Arc<Mutex<dyn Data + Send + Sync>>;

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
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()> {
        assert_eq!(self.expected_update.0, ino);
        assert_eq!(self.expected_update.1, data);
        Ok(())
    }
}

pub enum Node {
    Data(NodeData),
    Group(HashMap<String, u64>, u64)
}


pub enum Event {
    Mkdir{parent: u64, name: String, sender: Sender<Option<u64>>},
    Mk{parent: u64, name: String, sender: Sender<Option<u64>>},
    Rm{parent: u64, name: String, sender: Sender<bool>},
    Mv{parent: u64, new_parent: u64, name: String, new_name:String, sender: Sender<bool>},
    Rename{parent: u64, name: String, new_name:String, sender: Sender<bool>},
}

const TTL: Duration = Duration::from_secs(1);


pub struct ConfigFS {
    configuration: Arc<RwLock<Configuration>>,
    open: Arc<RwLock<HashMap<u64, Vec<u8>>>>,
    sender: UnboundedSender<Event>,
}

impl ConfigFS {
    pub async fn mount(name: &str, mount_path: &str, configuration: Arc<RwLock<Configuration>>) -> io::Result<(UnboundedReceiver<Event>, JoinHandle<io::Result<()>>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let fs = ConfigFS{configuration, open: Arc::new(RwLock::new(HashMap::new())), sender: tx};
        let mut mount_options = MountOptions::default();
        mount_options
            .force_readdir_plus(true)
            .fs_name(name);

        let handle = Session::new(mount_options)
            .mount_with_unprivileged(fs, mount_path)
            .await?;
        
        let join = tokio::spawn(async {
            let res = handle.await;
            res
        });
        
        Ok((rx, join))
    }

    async fn open(&self, data: Vec<u8>) -> u64 {
        let mut open = self.open.write().await;
        let mut rng = rand::thread_rng();
        let mut ino = rng.gen();
        while open.contains_key(&ino) {
            ino = rng.gen();
        }

        open.insert(ino, data);
        ino
    }

    async fn release(&self, fh: u64) -> Option<Vec<u8>> {
        let mut open = self.open.write().await;
        open.remove(&fh)
    }

    async fn create_attr(ino: u64, node: &Node) -> FileAttr {
        match node {
            Node::Data(node_data) => {
                let res = node_data.lock().await.fetch(ino).await.map(|d|d.len() as u64);
                ConfigFS::create_file_attr(ino, res.unwrap_or(0))
            },
            Node::Group(group, _) => ConfigFS::create_dir_attr(ino, group.len() as u32),
        }
    }
 
    fn create_file_attr(ino: u64, size: u64) -> FileAttr {
        FileAttr{
            ino: ino,
            generation: 0,
            size: size,
            blocks: 1,
            atime: UNIX_EPOCH.into(),
            mtime: UNIX_EPOCH.into(),
            ctime: UNIX_EPOCH.into(),
            kind: FileType::RegularFile,
            perm: fuse3::perm_from_mode_and_kind(FileType::RegularFile, 0o666),
            nlink: 1,
            uid: unsafe {
                libc::getuid()
            },
            gid: unsafe {
                libc::getegid()
            },
            rdev: 0,
            blksize: 512,
        }
    }

    fn create_dir_attr(ino: u64, files: u32) -> FileAttr {
        FileAttr{
            ino: ino,
            generation: 0,
            size: 0,
            blocks: 1,
            atime: UNIX_EPOCH.into(),
            mtime: UNIX_EPOCH.into(),
            ctime: UNIX_EPOCH.into(),
            kind: FileType::Directory,
            perm: fuse3::perm_from_mode_and_kind(FileType::Directory, 0o666),
            nlink: 2 + files,
            uid: unsafe {
                libc::getuid()
            },
            gid: unsafe {
                libc::getegid()
            },
            rdev: 0,
            blksize: 512,
        }
    }
}

#[cfg(test)]
mod test {
    
}