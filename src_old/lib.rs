use std::{collections::HashMap, sync::Arc, time::{Duration, UNIX_EPOCH}, io};

use fuse3::{raw::{reply::FileAttr, Session}, FileType, MountOptions};
use rand::Rng;
use tokio::{sync::{mpsc::{Sender, UnboundedReceiver, self, UnboundedSender}, RwLock, Mutex}, task::JoinHandle};

mod configuration;
mod fs;
pub mod basic;
pub mod serde;
mod tmp;
pub use configuration::Configuration;
pub use fuse3::{Result, async_trait};

#[async_trait]
pub trait Data {
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>>;
    async fn size(&mut self, ino: u64) -> Result<u64>;
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()>;
}

pub type NodeData = Arc<Mutex<dyn Data + Send + Sync>>;

pub enum EntryType {
    Data,
    Object
} 

#[async_trait]
pub trait Object: Data {
    async fn entires(&self, ino: u64) -> Result<Vec<(u64, EntryType)>>;
    async fn find(&self, ino: u64, path: Vec<String>) -> Result<(u64, EntryType)>;
    async fn lookup(&self, ino: u64, name: String) -> Result<(u64, EntryType)>;
    async fn contains(&self, ino: u64, name: String) -> bool;
    async fn contains_ino(&self, ino: u64) -> bool;

    async fn mk_data(&mut self, ino: u64, name: String) -> Result<u64>;
    async fn mk_obj(&mut self, ino: u64, name: String) -> Result<u64>;
    async fn mv(&mut self, parent: u64, new_parent: u64, name: String, new_name: String) -> Result<()>;
    async fn rm(&mut self, ino: u64, name: String) -> Result<()>;
    async fn rn(&mut self, ino: u64, name: String, new_name: String) -> Result<()>;
}

pub type NodeObject = Arc<RwLock<dyn Object + Send + Sync>>;

#[async_trait]
pub trait Lookup {
    async fn lookup(&self, ino: u64) -> bool;
    async fn redirect(&self, ino: u64) -> u64;
}

pub type InoLookup = Arc<RwLock<dyn Lookup + Sync + Send>>;

pub enum Node {
    Data(NodeData),
    Object(NodeObject),
    Group(HashMap<String, u64>, u64)
}


pub enum Event {
    MkGroup{parent: u64, name: String, sender: Sender<Option<u64>>},
    MkData{parent: u64, name: String, sender: Sender<Option<u64>>},
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
        let fs = ConfigFS{
            configuration, 
            open: Arc::new(RwLock::new(HashMap::new())), 
            sender: tx
        };
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
                let res = node_data.lock().await.size(ino).await;
                ConfigFS::create_file_attr(ino, res.unwrap_or(0))
            },
            Node::Object(obj) => {
                let res = obj.write().await.size(ino).await;
                ConfigFS::create_dir_attr(ino, res.unwrap_or(0) as u32 + 2)
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
    use crate::{basic::{EmptyInoLookup, StoredNodeData, StorageNodeObject}, configuration::ROOT};

    use super::*;
    use std::{fs::{self}, path::PathBuf};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() {
        fs::create_dir_all("tmp").unwrap();
        let tmp_mnt = TempDir::new_in("tmp").unwrap();
        println!("{:?}", tmp_mnt.path());
        let mnt = PathBuf::from(tmp_mnt.path());


        let (node_object, lookup) = StorageNodeObject::new(10);
        let config = Configuration::new(lookup);
        config.write().await.create_object(ROOT, "obj", 10, node_object).await.unwrap();
        let (mut events, mount_handle) = ConfigFS::mount(
            "test", 
            &mnt.to_string_lossy().to_string(), 
            config.clone()
        ).await.unwrap();       
        let node_data = StoredNodeData::new();
        
        let fs_handle = tokio::task::spawn_blocking(move || {
            let mut files = fs::read_dir(&mnt).unwrap();
            let Some(Ok(obj)) = files.next() else {
                panic!("expected a directory in root")
            };
            assert_eq!(obj.file_name(), "obj");
            assert!(matches!(files.next(), None));

            let dir = mnt.join("dir");
            fs::create_dir(&dir).unwrap();
            assert!(dir.exists());

            let dir2 = mnt.join("dir2");
            fs::create_dir(&dir2).unwrap();
            assert!(dir2.exists());

            let file1 = mnt.join("file1");
            fs::write(&file1, "testing").unwrap();
            assert!(file1.exists());

            assert_eq!(fs::read_to_string(&file1).unwrap(), "testing");

            let file2 = dir.join("file2");
            fs::write(&file2, "testing2").unwrap();
            assert!(file2.exists());

            assert_eq!(fs::read_to_string(&file2).unwrap(), "testing2");

            let new_file2 = dir2.join("file");
            fs::rename(&file2, &new_file2).unwrap();
            let file2 = new_file2;
            assert!(file2.exists());

            let new_file1 = mnt.join("file");
            fs::rename(&file1, &new_file1).unwrap();
            let file1 = new_file1;
            assert!(file1.exists());

            let new_dir2 = dir.join("dir2");
            fs::rename(&dir2, &new_dir2).unwrap();
            let dir2 = new_dir2;
            assert!(dir2.exists());
        });

        let event_handle = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::MkGroup { parent, name, sender } => {
                        let mut config = config.write().await;
                        match config.create_group(parent, &name).await {
                            Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                            Err(_) => {sender.send(None).await.unwrap();},
                        }
                    },
                    Event::MkData { parent, name, sender } => {
                        let mut config = config.write().await;
                        match config.create_data(
                            parent, 
                            &name, 
                            node_data.clone()
                        ).await {
                            Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                            Err(_) => {sender.send(None).await.unwrap();},
                        }
                    },
                    Event::Rm { parent, name, sender } => {
                        let mut config = config.write().await;
                        match config.remove(parent, &name).await {
                            Ok(_) => {sender.send(true).await.unwrap();},
                            Err(_) => {sender.send(false).await.unwrap();},
                        }
                    },
                    Event::Mv { parent, new_parent, name, new_name, sender } => {
                        let mut config = config.write().await;
                        match config.mv(parent, new_parent, &name, &new_name).await {
                            Ok(_) => {sender.send(true).await.unwrap();},
                            Err(_) => {sender.send(false).await.unwrap();},
                        }
                    },
                    Event::Rename { parent, name, new_name, sender } => {
                        let mut config = config.write().await;
                        match config.rename(parent, &name, &new_name).await {
                            Ok(_) => {sender.send(true).await.unwrap();},
                            Err(_) => {sender.send(false).await.unwrap();},
                        }
                    },
                }
            }
        });
        
        let (event_error, fs_error, mount_error) = tokio::join!(event_handle, fs_handle, mount_handle);
        if let Err(e) = event_error {
            println!("{}", e);
        }
        if let Err(e) = fs_error {
            println!("{}", e);
        }
        if let Err(e) = mount_error {
            println!("{}", e);
        }
    }
}