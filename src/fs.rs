use bytes::BytesMut;
use fuse3::{path::{PathFilesystem, reply::{DirectoryEntry, DirectoryEntryPlus, ReplyDirectoryPlus, ReplyCreated, ReplyEntry, ReplyAttr, FileAttr}, Session}, async_trait, raw::{Request, reply::{ReplyLSeek, ReplyWrite, ReplyData, ReplyOpen}}, SetAttr, FileType};
use futures_util::stream::{Empty, Iter};
use futures_util::{stream};
use fuse3::{Errno, MountOptions, Result};
use tokio::{sync::RwLock, task::JoinHandle};
use std::{vec::IntoIter, ffi::{OsStr, OsString}, time::{Duration, UNIX_EPOCH}, sync::Arc, collections::HashMap, io};
use rand::Rng;

use crate::{config::{EntryType}, mount::{FSMount, PathPair}};

const TTL: Duration = Duration::from_secs(1);

pub struct FS {
    mount: FSMount,
    open: Arc<RwLock<HashMap<u64, (String, Vec<u8>)>>>,
}

fn split_path<'a>(path: &'a str) -> Vec<&'a str> {
    path.split("/").into_iter().filter(|s| *s != "").collect()
}

fn create_attr(entry_type: &EntryType, size: u64) -> FileAttr {
    match entry_type {
        EntryType::Object => create_file_attr(size),
        EntryType::Data => create_dir_attr(size as u32),
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

impl FS {
    pub async fn mount(name: &str, mount_path: &str, mount: FSMount) -> io::Result<JoinHandle<io::Result<()>>> {
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
    type DirEntryStream = Empty<Result<DirectoryEntry>>;
    type DirEntryPlusStream = Iter<IntoIter<Result<DirectoryEntryPlus>>>;
    
    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let path: Vec<&str> = split_path(path);
        let (entry_type, size) = config.read().await.lookup(&path, &name.to_string_lossy().to_string()).await?;
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
            let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
            let path: Vec<&str> = split_path(path);
            let (entry_type, size) = config.read().await.lookup_path(&path).await?;
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
            let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
            let path: Vec<&str> = split_path(path);
            let (entry_type, size) = config.read().await.lookup_path(&path).await?;
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
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let parent: Vec<&str> = split_path(path);
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
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let parent: Vec<&str> = split_path(path);
        let mut config = config.write().await;
        config.rm(&parent, &name.to_string_lossy().to_string()).await?;
        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let parent: Vec<&str> = split_path(path);
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
        let mount = self.mount.read().await;
        
        let old_parent = origin_parent.to_string_lossy().to_string();
        let new_parent = parent.to_string_lossy().to_string();

        let old_name = origin_name.to_string_lossy().to_string();
        let new_name = name.to_string_lossy().to_string();

        let (config, path) = mount.resolve(&old_parent).ok_or_else(|| Errno::new_not_exist())?;
        let mut config = config.write().await;

        if old_parent == new_parent {
            let old_parent: Vec<&str> = split_path(path);
            config.rn(&old_parent, &old_name, &new_name).await?;
        } else {
            let (configs, old_parent, new_parent) = mount.resolve_pair(&old_parent, &new_parent).ok_or_else(|| Errno::new_not_exist())?;

            let mut old_parent: Vec<&str> = split_path(old_parent);
            let mut new_parent: Vec<&str> = split_path(new_parent);

            match configs {
                PathPair::Single(config) => {
                    let mut config = config.write().await;
                    config.mv(&old_parent, &new_parent, &old_name, &new_name).await?;
                },
                PathPair::Pair(config_a, config_b) => {
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
                        config.update(&new_parent, data).await?;
                    }
                },
            }
        }

        Ok(())
    }

    async fn open(&self, _req: Request, path: &OsStr, _flags: u32) -> Result<ReplyOpen> {
        let mount = self.mount.read().await;
        let path_str = path.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&path_str).ok_or_else(|| Errno::new_not_exist())?;
        let path: Vec<&str> = split_path(path);
        let mut config = config.write().await;

        let (entry_type, _) = config.lookup_path(&path).await?; 
        if !matches!(entry_type, EntryType::Data) {
            return Err(Errno::new_is_dir())
        }

        let data = config.fetch(&path).await?;
        let fh = self.open(&path_str, data).await;
        Ok(ReplyOpen { 
            fh, 
            flags: 0
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
        let Some((path, data)) = self.release(fh).await else {
            return Err(Errno::new_not_exist())
        };

        let mount = self.mount.read().await;
        let (config, path) = mount.resolve(&path).ok_or_else(|| Errno::new_not_exist())?;
        let path: Vec<&str> = split_path(path);
        let mut config = config.write().await;
        config.update(&path, data).await?;

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
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let file_path = format!("{}/{}", parent, name.to_string_lossy().to_string());
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let parent: Vec<&str> = split_path(path);
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

    async fn readdirplus(
        &self,
        _req: Request,
        parent: &OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream>> {
        let mount = self.mount.read().await;
        let parent = parent.to_string_lossy().to_string();
        let (config, path) = mount.resolve(&parent).ok_or_else(|| Errno::new_not_exist())?;
        let parent: Vec<&str> = split_path(path);
        let config = config.read().await;

        let entries = config.entires(&parent).await?;
        let mut children = Vec::with_capacity(2 + entries.len());

        children.extend(vec![
                (FileType::Directory, OsString::from("."), create_dir_attr(entries.len( )as u32), 1),
                (FileType::Directory, OsString::from(".."), create_dir_attr(2), 2),
        ]);
        
        for (i, name) in entries.iter().enumerate() {
            if let Ok((entry_type, size)) = config.lookup(&parent, name).await {
                let attr = create_attr(&entry_type, size);
                children.push((attr.kind, OsString::from(name), attr, i as i64 + 3));
            }
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
        drop(config);
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
        _fh: u64,
        offset: u64,
        _whence: u32,
    ) -> Result<ReplyLSeek> {
        Ok(ReplyLSeek { offset })
    }
}