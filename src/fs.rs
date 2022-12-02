use bytes::BytesMut;
use fuse3::{path::{PathFilesystem, reply::{DirectoryEntry, DirectoryEntryPlus, ReplyDirectoryPlus, ReplyCreated, ReplyEntry, ReplyAttr, FileAttr}, Session}, async_trait, raw::{Request, reply::{ReplyLSeek, ReplyWrite, ReplyData, ReplyOpen}}, SetAttr, FileType};
use futures_util::stream::{Iter};
use futures_util::{stream};
use fuse3::{Errno, MountOptions, Result};
use tokio::{sync::RwLock, task::JoinHandle};
use std::{vec::IntoIter, ffi::{OsStr, OsString}, time::{Duration, UNIX_EPOCH}, sync::Arc, collections::HashMap, io};
use rand::Rng;

use crate::{config::{EntryType, Configuration}, mount::{FSMount, PathPair}};

const TTL: Duration = Duration::from_secs(1);

pub struct FS {
    mount: FSMount,
    open: Arc<RwLock<HashMap<u64, (String, Vec<u8>)>>>,
}

pub(crate) fn split_path<'a>(path: &'a str) -> Vec<&'a str> {
    path.split("/").into_iter().filter(|s| *s != "").collect()
}

fn create_attr(entry_type: &EntryType, size: u64) -> FileAttr {
    match entry_type {
        EntryType::Data => create_file_attr(size),
        EntryType::Object => create_dir_attr(size as u32),
    }
}

fn create_file_attr(size: u64) -> FileAttr {
    FileAttr{
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

fn create_dir_attr(files: u32) -> FileAttr {
    FileAttr{
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

/// # FS
/// Handles IO requests and routes them to config interfaces.
impl FS {
    /// Mount the filesystem. Returns a join handle to the thread handling io requests.
    pub async fn mount(name: &str, mount_path: &str, mount: FSMount) -> io::Result<JoinHandle<io::Result<()>>> {
        let ticker_handles = mount.write().await.spawn_tickers().await;
        let fs = FS{
            mount,
            open: Arc::new(RwLock::new(HashMap::new())), 
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
            for handle in ticker_handles {
                handle.await.ok();
            }
            res
        });
        Ok(join)
    }

    async fn open(&self, path: &str, data: Vec<u8>) -> u64 {
        let mut open = self.open.write().await;
        let mut rng = rand::thread_rng();
        let mut ino = rng.gen();
        while open.contains_key(&ino) {
            ino = rng.gen();
        }

        open.insert(ino, (path.to_string(), data));
        ino
    }

    async fn release(&self, fh: u64) -> Option<(String, Vec<u8>)> {
        let mut open = self.open.write().await;
        open.remove(&fh)
    }
}

#[async_trait]
impl PathFilesystem for FS {
    type DirEntryStream = Iter<IntoIter<Result<DirectoryEntry>>>;
    type DirEntryPlusStream = Iter<IntoIter<Result<DirectoryEntryPlus>>>;
    
    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        println!("LOOKUP {:?} {:?}", parent, name);
        let mount = self.mount.read().await;
        let parent = format!("{}/{}", parent.to_string_lossy().to_string(), name.to_string_lossy().to_string());
        let Some((config, path)) = mount.resolve(&parent) else {
            let Some(entires) = mount.entries(&parent) else {
                return Err(Errno::new_not_exist())
            };

            return Ok(ReplyEntry{
                ttl: TTL,
                attr: create_attr(&EntryType::Object, entires.len() as u64)
            })
        };
        let (entry_type, size) = match config{
            Configuration::Basic(basic) => (EntryType::Data, basic.write().await.size().await?),
            Configuration::Complex(complex) => complex.read()
                .await
                .lookup_path(&path)
                .await?,
        };
        Ok(ReplyEntry{
            ttl: TTL,
            attr: create_attr(&entry_type, size)
        })
    }

    async fn forget(&self, _req: Request, _parent: &OsStr, _nlookup: u64) {}

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        println!("GETATTR {:?}, {:?}", path, fh);
        if let Some(fh) = fh  {
            let open = self.open.read().await;
            let (_, data) = open.get(&fh).ok_or_else(|| Errno::new_not_exist())?;
            Ok(
                ReplyAttr{
                    ttl: TTL,
                    attr: create_file_attr(data.len() as u64)
                }
            )
        }  else {
            let mount = self.mount.read().await;
            let parent = path.ok_or_else(|| Errno::new_not_exist())?.to_string_lossy().to_string();
            let Some((config, path)) = mount.resolve(&parent) else {
                let Some(entires) = mount.entries(&parent) else {
                    return Err(Errno::new_not_exist())
                };
    
                return Ok(ReplyAttr{
                    ttl: TTL,
                    attr: create_attr(&EntryType::Object, entires.len() as u64)
                })
            };
            let (entry_type, size) = match config{
                Configuration::Basic(basic) => (EntryType::Data, basic.write().await.size().await?),
                Configuration::Complex(complex) => complex.read()
                    .await
                    .lookup_path(&path)
                    .await?,
            };
            Ok(
                ReplyAttr{
                    ttl: TTL,
                    attr: create_attr(&entry_type, size)
                }
            )
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        println!("SETATTR");
        if let Some(fh) = fh  {
            let open = self.open.read().await;
            let (_, data) = open.get(&fh).ok_or_else(|| Errno::new_not_exist())?;
            Ok(
                ReplyAttr{
                    ttl: TTL,
                    attr: create_file_attr(data.len() as u64)
                }
            )
        }  else {
            let mount = self.mount.read().await;
            let parent = path.ok_or_else(|| Errno::new_not_exist())?.to_string_lossy().to_string();
            let Some((config, path)) = mount.resolve(&parent) else {
                let Some(entires) = mount.entries(&parent) else {
                    return Err(Errno::new_not_exist())
                };
    
                return Ok(ReplyAttr{
                    ttl: TTL,
                    attr: create_attr(&EntryType::Object, entires.len() as u64)
                })
            };
            let (entry_type, size) = match config{
                Configuration::Basic(basic) => (EntryType::Data, basic.write().await.size().await?),
                Configuration::Complex(complex) => complex.read()
                    .await
                    .lookup_path(&path)
                    .await?,
            };
            Ok(
                ReplyAttr{
                    ttl: TTL,
                    attr: create_attr(&entry_type, size)
                }
            )
        }
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        println!("MKDIR");
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, parent) = mount.resolve(&parent).ok_or_else(|| Errno::from(libc::EACCES))?;

        let Configuration::Complex(config) = config else {
            return Err(Errno::new_is_not_dir())
        };
        let mut config = config.write().await;
        config.mk_obj(&parent, &name.to_string_lossy().to_string()).await?;
        Ok(
            ReplyEntry{
                ttl: TTL,
                attr: create_attr(&EntryType::Object, 0)
            }
        )
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        println!("UNLINK");
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, parent) = mount.resolve(&parent).ok_or_else(|| Errno::from(libc::EACCES))?;

        let Configuration::Complex(config) = config else {
            return Err(libc::EACCES.into())
        };
        let mut config = config.write().await;
        config.rm(&parent, &name.to_string_lossy().to_string()).await?;
        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        println!("RMDIR");
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, parent) = mount.resolve(&parent).ok_or_else(|| Errno::from(libc::EACCES))?;

        let Configuration::Complex(config) = config else {
            return Err(Errno::new_is_not_dir())
        };
        let mut config = config.write().await;
        config.rm(&parent, &name.to_string_lossy().to_string()).await?;
        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        println!("RENAME");
        let mount = self.mount.read().await;
        
        let old_parent = origin_parent.to_string_lossy().to_string();
        let new_parent = parent.to_string_lossy().to_string();

        let old_name = origin_name.to_string_lossy().to_string();
        let new_name = name.to_string_lossy().to_string();


        if old_parent == new_parent {
            let (config, old_parent) = mount.resolve(&old_parent).ok_or_else(|| Errno::from(libc::EACCES))?;
            let Configuration::Complex(config) = config else {
                return Err(Errno::new_is_not_dir())
            };
            let mut config = config.write().await;

            config.rn(&old_parent, &old_name, &new_name).await?;
        } else {
            let (configs, mut old_parent, mut new_parent) = mount.resolve_pair(&old_parent, &new_parent).ok_or_else(|| Errno::new_not_exist())?;

            match configs {
                PathPair::Single(config) => {
                    let Configuration::Complex(config) = config else {
                        return Err(Errno::new_is_not_dir())
                    };
                    
                    let mut config = config.write().await;
                    config.mv(&old_parent, &new_parent, &old_name, &new_name).await?;
                },
                PathPair::Pair(config_a, config_b) => {
                    let Configuration::Complex(config_a) = config_a else {
                        return Err(Errno::new_is_not_dir())
                    };
                    let Configuration::Complex(config_b) = config_b else {
                        return Err(Errno::new_is_not_dir())
                    };
            
                    if Arc::ptr_eq(&config_a, &config_b) {
                        let mut config = config_a.write().await;
                        config.mv(&old_parent, &new_parent, &old_name, &new_name).await?;
                    } else {
                        let mut config_a = config_a.write().await;
                        let mut config_b = config_b.write().await;
    
                        if config_b.contains(&new_parent, &new_name).await {
                            return Err(Errno::new_exist());
                        }
    
                        old_parent.push(&old_name);
                        let data = config_a.fetch(&old_parent).await?;
                        old_parent.pop();
                        config_a.rm(&old_parent, &old_name).await?;
    
                        config_b.mk_data(&new_parent, &new_name).await?;
                        new_parent.push(&new_name);
                        config_b.update(&new_parent, data).await?;
                    }
                },
            }
        }

        Ok(())
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        println!("OPEN {:?}", path);
        let mount = self.mount.read().await;
        let path_str = path.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&path_str).ok_or_else(|| Errno::from(libc::EACCES))?;

        let data = match config{
            Configuration::Basic(basic) => basic.write().await.fetch().await?,
            Configuration::Complex(complex) =>  {
                let mut config = complex.write().await;
                let (entry_type, _) = config.lookup_path(&path).await?; 
                println!("OPEN 2 {:?}", path);
                if !matches!(entry_type, EntryType::Data) {
                    return Err(Errno::new_is_dir())
                }
                config.fetch(&path).await?
            },
        };
        
        let fh = if libc::O_TRUNC & (flags as i32) != 0 {
            self.open(&path_str, vec![]).await
        } else {
            self.open(&path_str, data).await
        };

        Ok(ReplyOpen { 
            fh, 
            flags
        })
    }

    async fn read(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        println!("READ");
        let open = self.open.read().await;

        let Some((_, data)) = open.get(&fh) else {
            return Err(Errno::new_not_exist())
        };

        if offset > data.len() as u64 {
            Ok(ReplyData{
                data: "".into()
            })
        } else if offset + size as u64 > data.len() as u64 {
            let mut bytes = BytesMut::with_capacity(size as usize);
            bytes.extend_from_slice(&data[offset as usize..]);
            Ok(ReplyData{
                data: bytes.into()
            })
        } else {
            let mut bytes = BytesMut::with_capacity(size as usize);
            bytes.extend_from_slice(&data[offset as usize..size as usize]);
            Ok(ReplyData{
                data: bytes.into()
            })
        }
    }

    async fn write(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
    ) -> Result<ReplyWrite> {
        println!("WRITE");
        let mut open = self.open.write().await;

        let Some((_, file_data)) = open.get_mut(&fh) else {
            return Err(Errno::new_not_exist())
        };

        let offset = offset as usize;
        let mut i = 0;

        while i + offset < file_data.len() && i < data.len() {
            file_data[(i+offset) as usize] = data[i as usize];
            i += 1;
        }

        if (i as usize) < data.len() {
            file_data.extend_from_slice(&data[i..]);
        }

        Ok(ReplyWrite{
            written: data.len() as u32
        })
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        println!("RELEASE");
        let Some((path, data)) = self.release(fh).await else {
            return Err(Errno::new_not_exist())
        };

        let mount = self.mount.read().await;
        let (config, path) = mount.resolve(&path).ok_or_else(|| Errno::from(libc::EACCES))?;

        match config{
            Configuration::Basic(basic) => basic.write().await.update(data).await?,
            Configuration::Complex(complex) => complex.write().await.
                update(&path, data).await?,
        }
        Ok(())
    }

    async fn fsync(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> Result<ReplyCreated> {
        println!("CREATE {:?} {:?}", parent, name);
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let file_path = format!("{}/{}", parent, name.to_string_lossy().to_string());
        let (config, parent) = mount.resolve(&parent).ok_or_else(|| Errno::from(libc::EACCES))?;
        let Configuration::Complex(config) = config else {
            return Err(Errno::new_is_not_dir())
        };
        let mut config = config.write().await;

        config.mk_data(&parent, &name.to_string_lossy().to_string()).await?;
        let fh = self.open(&file_path, vec![]).await;

        Ok(ReplyCreated { 
            ttl: TTL, 
            attr: create_file_attr(0), 
            generation: 0, 
            fh, 
            flags: 0
        })
    }

    async fn batch_forget(&self, _req: Request, _paths: &[&OsStr]) {}

    // Not supported by fusefs(5) as of FreeBSD 13.0
    #[cfg(target_os = "linux")]
    async fn fallocate(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _offset: u64,
        _length: u64,
        _mode: u32,
    ) -> Result<()> {
       Ok(())
    }

    async fn opendir(
        &self,
        _req: Request,
        _path: &OsStr,
        _flags: u32
    ) -> Result<ReplyOpen> {
        Ok(ReplyOpen{
            fh: 0,
            flags: 0,
        })
    }
    
    async fn readdirplus(
        &self,
        _req: Request,
        parent: &OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream>> {
        println!("READDIR");
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let entries = if let Some((config, parent)) = mount.resolve(&parent) {
            let Configuration::Complex(config) = config else {
                return Err(Errno::new_is_not_dir())
            };
            let config = config.read().await;
    
            let entry_names = config.entires(&parent).await?;
            let mut entries = Vec::with_capacity(entry_names.len());

            for name in entry_names.into_iter() {
                if let Ok((entry_type, size)) = config.lookup(&parent, &name).await {
                    entries.push((name, entry_type, size));
                }
            }
            entries
        } else {
            let Some(entries) = mount.entries(&parent) else {
                return Err(Errno::new_not_exist())
            };
            entries.into_iter().map(|(n, t)| (n, t, 2)).collect()
        };
       
        let mut children = Vec::with_capacity(2 + entries.len());

        children.extend(vec![
                (FileType::Directory, OsString::from("."), create_dir_attr(entries.len( )as u32), 1),
                (FileType::Directory, OsString::from(".."), create_dir_attr(2), 2),
        ]);

        for (i, (name, entry_type, size)) in entries.iter().enumerate() {
                let attr = create_attr(&entry_type, *size);
                children.push((attr.kind, OsString::from(name), attr, i as i64 + 3));
        }
        

        let children = children.into_iter()
        .map(|(kind, name, attr, offset)| DirectoryEntryPlus {
                kind,
                name,
                offset,
                attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            })
            .skip(offset as _)
            .map(Ok)
            .collect::<Vec<_>>();

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(children),
        })
    }

    async fn rename2(
        &self,
        req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.rename(req, origin_parent, origin_name, parent, name)
            .await
    }

    async fn lseek(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        print!("LSEEK");
        let open = self.open.read().await;
        let Some((_, data)) = open.get(&fh) else {
            return Err(Errno::new_not_exist())
        };
        let size = data.len();
        
        let whence = whence as i32;

        let offset = if whence == libc::SEEK_CUR || whence == libc::SEEK_SET {
            offset
        } else if whence == libc::SEEK_END {
            if size >= offset as _ {
                size as u64 - offset
            } else {
                0
            }
        } else {
            return Err(libc::EINVAL.into());
        };

        Ok(ReplyLSeek { offset })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::{self}, io::{ErrorKind, Write}, time::Duration, sync::Arc};

    use fuse3::{Errno, async_trait};
    use tokio::sync::RwLock;

    use crate::{mount::Mount, basic::mem::{MemDir, MemFile}, BasicConfigHook, Result, Configuration};

    use super::FS;

    struct Ticker;

    impl Ticker {
        pub fn new() -> Configuration {
            Configuration::Basic(Arc::new(RwLock::new(Ticker)))
        }
    }
    
    #[async_trait]
    impl BasicConfigHook for Ticker {
        async fn fetch(&mut self) -> Result<Vec<u8>> {
            Err(Errno::new_not_exist())
        }
        async fn size(&mut self) -> Result<u64> {
            Err(Errno::new_not_exist())
        }
        async fn update(&mut self, _data: Vec<u8>) -> Result<()> {
            Err(Errno::new_not_exist())
        }
        
        async fn tick(&mut self) {
            println!("ticked {:p}", self);
        }
        fn tick_interval(&self) -> Duration {
            Duration::from_millis(500)
        }

    }
    

    #[tokio::test] 
    async fn test() {
        fs::create_dir_all("tmp").unwrap();
        let tmp =tempfile::TempDir::new_in("tmp").unwrap();
        println!("{:?}", tmp.path());
        let ticker = Ticker::new();
        let mount = Mount::new();
        {
            let mut mnt = mount.write().await;
            mnt.mount("/dir", MemDir::new());
            mnt.mount("/file", MemFile::new());
            mnt.mount("/ticker1", ticker.clone());
            mnt.mount("/ticker1_clone", ticker);
            mnt.mount("/ticker2", Ticker::new());
        }
        let fs = FS::mount("test", tmp.path().to_str().unwrap(), mount);
        let mount_handle = fs.await.unwrap();

        let sort = |mut v: Vec<_>| {v.sort(); v};

        tokio::task::spawn_blocking(move || {
            let path = tmp.path();
            assert_eq!(fs::create_dir(&path.join("tmp")).unwrap_err().kind(), ErrorKind::PermissionDenied);

            let dir = path.join("dir");
            assert_eq!(
                sort(fs::read_dir(&dir).unwrap()
                    .into_iter()
                    .map(|d| d.unwrap().path().to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                ),
                Vec::<&str>::new()
            );
    
            let dir2 = dir.join("dir");
            fs::create_dir(&dir2).unwrap();
            assert!(dir2.exists());        
    
            let file = dir.join("file");
            fs::File::create(&file).unwrap().write("hello".as_bytes()).unwrap();
            assert!(file.exists());   
            assert_eq!(fs::read_to_string(&file).unwrap(), "hello");        
    
            let file_new_name = dir.join("file+");
            fs::rename(&file, &file_new_name).unwrap();
            assert!(!file.exists());   
            assert!(file_new_name.exists());   
            let file = file_new_name;
    
            let file_path = dir2.join("file");
            fs::rename(&file, &file_path).unwrap();
            assert!(!file.exists());   
            assert!(file_path.exists());   
            let file = file_path;
    
            fs::remove_file(&file).unwrap();
            assert!(!file.exists());
    
            fs::remove_dir(&dir2).unwrap();
            assert!(!dir2.exists());
            
            let file = path.join("file");
            fs::File::create(&file).unwrap().write("hello".as_bytes()).unwrap();
            assert!(file.exists());   
            assert_eq!(fs::read_to_string(&file).unwrap(), "hello"); 
        }).await.unwrap();
        

        mount_handle.await.unwrap().unwrap();
    }
}