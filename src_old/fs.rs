use std::{vec::IntoIter, ffi::{OsStr, OsString}};

use bytes::BytesMut;
use fuse3::{Result, raw::{Filesystem, reply::{DirectoryEntry, DirectoryEntryPlus, ReplyEntry, ReplyAttr, ReplyOpen, ReplyData, ReplyWrite, ReplyCreated, ReplyDirectoryPlus, ReplyLSeek}, Request}, async_trait, Errno, FileType, SetAttr};
use futures_util::stream::{Empty, Iter, self};
use tokio::sync::mpsc;
use crate::{Event, Node, TTL, ConfigFS};

#[async_trait]
impl Filesystem for ConfigFS {
    type DirEntryStream = Empty<Result<DirectoryEntry>>;
    type DirEntryPlusStream = Iter<IntoIter<Result<DirectoryEntryPlus>>>;

    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        println!("LOOKUP");
        let config = self.configuration.read().await;
        let child_ino = config.get_child(parent, &name.to_string_lossy().to_string()).await?;
        let child = config.get(child_ino).await?;

        Ok(ReplyEntry {
            ttl: TTL,
            attr: ConfigFS::create_attr(child_ino, child).await,
            generation: 0,
        })
    }

    async fn forget(&self, _req: Request, _inode: u64, _nlookup: u64) {}

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        println!("GETATTR");
        Ok(ReplyAttr {
            ttl: TTL,
            attr: {
                let config = self.configuration.read().await;
                let child = config.get(inode).await?;
                ConfigFS::create_attr(inode, child).await
            },
        })
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        println!("SETATTR");
        Ok(ReplyAttr { 
            ttl: TTL, 
            attr: {
                let config = self.configuration.read().await;
                let child = config.get(inode).await?;
                ConfigFS::create_attr(inode, child).await
            },
        })
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        println!("MKDIR");
        let config = self.configuration.read().await;
        let ino = match config.get(parent).await? {
            Node::Object(obj) =>  {
                let mut obj = obj.write().await;
                let ino = obj.mk_obj(parent, name.to_string_lossy().to_string()).await?;
                drop(obj);
                drop(config);
                ino
            },
            Node::Data(_) => return Err(Errno::new_is_not_dir()),
            Node::Group(_, _) => {
                drop(config);

                let (tx, mut rx) = mpsc::channel(1);
                let Ok(_) = self.sender.send(Event::MkGroup { parent, name: name.to_string_lossy().to_string(), sender: tx }) else {
                    return Err(libc::ENOTCONN.into())
                };

                let Some(Some(ino)) = rx.recv().await else {
                    return Err(libc::EACCES.into())
                };
                ino
            },
        };
        
        Ok(ReplyEntry { ttl: TTL, attr: ConfigFS::create_dir_attr(ino, 0), generation: 0 })
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        println!("UNLINK");
        let config = self.configuration.read().await;
        let child_ino = config.get_child(parent, &name.to_string_lossy().to_string()).await?;

        match config.get(child_ino).await? {
            Node::Object(obj) =>  {
                let mut obj = obj.write().await;
                obj.rm(parent, name.to_string_lossy().to_string()).await?;
                drop(obj);
                drop(config);
            },
            Node::Data(_) => {
                drop(config);
                let (tx, mut rx) = mpsc::channel(1);
                let Ok(_) = self.sender.send(Event::Rm { parent, name: name.to_string_lossy().to_string(), sender: tx }) else {
                    return Err(libc::ENOTCONN.into())
                };

                let Some(true) = rx.recv().await else {
                    return Err(libc::EACCES.into())
                };
            },
            Node::Group(_, _) => return Err(Errno::new_is_dir()),
        }

        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        println!("RMDIR");
        let config = self.configuration.read().await;
        let child_ino = config.get_child(parent, &name.to_string_lossy().to_string()).await?;

        match config.get(child_ino).await? {
            Node::Object(obj) =>  {
                let mut obj = obj.write().await;
                obj.rm(parent, name.to_string_lossy().to_string()).await?;
                drop(obj);
                drop(config)
            },
            Node::Data(_) => return Err(Errno::new_is_not_dir()),
            Node::Group(_, _) => {
                drop(config);
                let (tx, mut rx) = mpsc::channel(1);
                let Ok(_) = self.sender.send(Event::Rm { parent, name: name.to_string_lossy().to_string(), sender: tx }) else {
                    return Err(libc::ENOTCONN.into())
                };

                let Some(true) = rx.recv().await else {
                    return Err(libc::EACCES.into())
                };
            },
        }

        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<()> {
        println!("RENAME");
        let config = self.configuration.read().await;
        config.get_child(parent, &name.to_string_lossy().to_string()).await?;


        match config.get(parent).await? {
            Node::Object(obj) =>  {
                let mut obj = obj.write().await;
                if parent == new_parent {
                    obj.rn(parent, name.to_string_lossy().to_string(), new_name.to_string_lossy().to_string()).await?;
                } else {
                    obj.mv(parent, new_parent, name.to_string_lossy().to_string(), new_name.to_string_lossy().to_string()).await?;
                }
                drop(obj);
                drop(config);
            },
            Node::Data(_) => return Err(Errno::new_is_not_dir()),
            Node::Group(_, _) => {
                drop(config);
                if parent == new_parent {
                    let (tx, mut rx) = mpsc::channel(1);
                    let Ok(_) = self.sender.send(Event::Rename { 
                        parent, 
                        name: name.to_string_lossy().to_string(),
                        new_name: new_name.to_string_lossy().to_string(),
                        sender: tx 
                    }) else {
                        return Err(libc::ENOTCONN.into())
                    };
        
                    let Some(true) = rx.recv().await else {
                        return Err(libc::EACCES.into())
                    };
                } else {
                    let (tx, mut rx) = mpsc::channel(1);
                    let Ok(_) = self.sender.send(Event::Mv { 
                        parent, 
                        new_parent, 
                        name: name.to_string_lossy().to_string(),
                        new_name: new_name.to_string_lossy().to_string(),
                        sender: tx 
                    }) else {
                        return Err(libc::ENOTCONN.into())
                    };
        
                    let Some(true) = rx.recv().await else {
                        return Err(libc::EACCES.into())
                    };
                }
            },
        }

        Ok(())
    }

    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> Result<ReplyOpen> {
        println!("OPEN");
        let config = self.configuration.read().await;
        println!("OPEN2");
        let data = config.fetch_data(inode).await?;
        println!("OPEN3");
        
        let fh = self.open(data).await;

        Ok(ReplyOpen{
            fh,
            flags: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        println!("READ");
        let open = self.open.read().await;

        let Some(data) = open.get(&fh) else {
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
        _inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
    ) -> Result<ReplyWrite> {
        println!("WRITE");
        let mut open = self.open.write().await;

        let Some(file_data) = open.get_mut(&fh) else {
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
        inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        println!("RELEASE");
        let Some(data) = self.release(fh).await else {
            return Err(Errno::new_not_exist())
        };

        let config = self.configuration.read().await;
        config.update_data(inode, data).await?;

        Ok(())
    }

    async fn fsync(&self, _req: Request, _inode: u64, _fh: u64, _datasync: bool) -> Result<()> {
        Ok(())
    }

    async fn flush(&self, _req: Request, _inode: u64, _fh: u64, _lock_owner: u64) -> Result<()> {
        Ok(())
    }

    async fn access(&self, _req: Request, _inode: u64, _mask: u32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> Result<ReplyCreated> {
        println!("CREATE");
        let config = self.configuration.write().await;
        let ino = match config.get(parent).await? {
            Node::Object(obj) =>  {
                println!("CREATE 1");
                let mut obj = obj.write().await;
                println!("CREATE 2");
                let ino = obj.mk_data(parent, name.to_string_lossy().to_string()).await?;
                drop(obj);
                drop(config);
                ino
            },
            Node::Data(_) => return Err(Errno::new_is_not_dir()),
            Node::Group(_, _) => {
                drop(config);
                let (tx, mut rx) = mpsc::channel(1);
                let Ok(_) = self.sender.send(Event::MkData { parent, name: name.to_string_lossy().to_string(), sender: tx }) else {
                    return Err(libc::ENOTCONN.into())
                };

                let Some(Some(ino)) = rx.recv().await else {
                    return Err(libc::EACCES.into())
                };
                ino
            },
        };

        let config = self.configuration.read().await;

        let data = config.fetch_data(ino).await?;
        let size = data.len() as u64;
        
        let fh = self.open(data).await;

        Ok(ReplyCreated{
            ttl: TTL,
            attr: ConfigFS::create_file_attr(ino, size),
            generation: 0,
            flags: 0,
            fh
        })
    }

    async fn interrupt(&self, _req: Request, _unique: u64) -> Result<()> {
        Ok(())
    }

    async fn fallocate(
        &self,
        _req: Request,
        _inode: u64,
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
        dir: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream>> {
        println!("READDIR");
        let config = self.configuration.read().await;


        let children = match config.get(dir).await? {
            Node::Object(obj) =>  {
                let obj = obj.read().await;
                let group =  obj.entires(dir).await?;

                let mut children = Vec::with_capacity(2 + group.len());

                children.extend(vec![
                        (dir, FileType::Directory, OsString::from("."), ConfigFS::create_dir_attr(dir, group.len()as u32), 1),
                        (*parent, FileType::Directory, OsString::from(".."), ConfigFS::create_dir_attr(*parent, 2), 2),
                ]);
                
                for (i, (name, ino)) in group.iter().enumerate() {
                    if let Ok(node) = config.get(*ino).await {
                        let attr = ConfigFS::create_attr(*ino, node).await;
                        children.push((*ino, attr.kind, OsString::from(name), attr, i as i64 + 3));
                    }
                }
                drop(obj);
                drop(config);
            },
            Node::Data(_) => return Err(Errno::new_is_not_dir()),
            Node::Group(group, parent) => {
                let mut children = Vec::with_capacity(2 + group.len());

                children.extend(vec![
                        (dir, FileType::Directory, OsString::from("."), ConfigFS::create_dir_attr(dir, group.len()as u32), 1),
                        (*parent, FileType::Directory, OsString::from(".."), ConfigFS::create_dir_attr(*parent, 2), 2),
                ]);
                
                for (i, (name, ino)) in group.iter().enumerate() {
                    if let Ok(node) = config.get(*ino).await {
                        let attr = ConfigFS::create_attr(*ino, node).await;
                        children.push((*ino, attr.kind, OsString::from(name), attr, i as i64 + 3));
                    }
                }

                drop(config);

                children
            },
        };
        

        let children = children.into_iter()
            .map(|(inode, kind, name, attr, offset)| DirectoryEntryPlus {
                inode,
                generation: 0,
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
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.rename(req, parent, name, new_parent, new_name).await
    }

    async fn lseek(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        _offset: u64,
        _whence: u32,
    ) -> Result<ReplyLSeek> {
        println!("LSEEK");
        let config = self.configuration.read().await;
        let Node::Data(_) = config.get(inode).await? else {
            return Err(Errno::new_not_exist())
        };
        
        Err(libc::EINVAL.into())
    }
}
