use std::{collections::HashMap, sync::Arc, time::{Duration, UNIX_EPOCH}, io};

use fuse3::{Result, raw::{reply::FileAttr, Session}, FileType, MountOptions};
use rand::Rng;
use tokio::sync::{mpsc::{Sender, UnboundedReceiver, self, UnboundedSender}, RwLock};

mod configuration;
mod fs;
pub use configuration::Configuration;

pub type Fetch = Arc<dyn Fn(u64) -> Result<Vec<u8>> + Send + Sync>;
pub type Update = Arc<dyn Fn(u64, Vec<u8>) -> Result<()> + Send + Sync>;
pub enum Node {
    Data(Fetch, Update),
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
    pub async fn mount(name: &str, mount_path: &str, configuration: Arc<RwLock<Configuration>>) -> io::Result<UnboundedReceiver<Event>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let fs = ConfigFS{configuration, open: Arc::new(RwLock::new(HashMap::new())), sender: tx};
        let mut mount_options = MountOptions::default();
        mount_options
            .force_readdir_plus(true)
            .fs_name(name);

        let handle = Session::new(mount_options)
            .mount_with_unprivileged(fs, mount_path)
            .await?;
        
        tokio::spawn(async {
            handle.await
        });
        
        Ok(rx)
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

    fn create_attr(ino: u64, node: &Node) -> FileAttr {
        match node {
            Node::Data(fetch, _) => {
                let res =fetch(ino).map(|d|d.len() as u64);
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
    use super::*;

    #[tokio::test]
    async fn test() {
        let config = Configuration::new();
        let mut events = ConfigFS::mount("test", "mnt", config.clone()).await.unwrap();

        while let Some(event) = events.recv().await {
            match event {
                Event::Mkdir { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.create_group(parent, &name) {
                        Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                        Err(_) => {sender.send(None).await.unwrap();},
                    }
                },
                Event::Mk { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.create_file(
                        parent, 
                        &name, 
                        Arc::new(|ino| Ok(format!("hello{}", ino).as_bytes().to_vec())),
                        Arc::new(|ino, data| {println!("{}: '{}'", ino, String::from_utf8(data).unwrap()); Ok(())})
                    ) {
                        Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                        Err(_) => {sender.send(None).await.unwrap();},
                    }
                },
                Event::Rm { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.remove(parent, &name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
                Event::Mv { parent, new_parent, name, new_name, sender } => {
                    let mut config = config.write().await;
                    match config.mv(parent, new_parent, &name, &new_name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
                Event::Rename { parent, name, new_name, sender } => {
                    let mut config = config.write().await;
                    match config.rename(parent, &name, &new_name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
            }
        }
    }
}