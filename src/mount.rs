use std::{sync::Arc, iter};

use tokio::sync::RwLock;

use crate::{config::Configuration, fs::split_path};


pub type FSMount = Arc<RwLock<Mount>>;

pub(crate) enum PathPair {
    Single(Configuration),
    Pair(Configuration, Configuration)
}

pub struct Mount {
    mounts: Vec<(Vec<String>, Configuration)>,
}

impl Mount {
    pub fn new() -> FSMount {
        Arc::new(RwLock::new(Mount{mounts: Vec::new()}))
    }

    fn eq(mount: &Vec<String>, path: &Vec<&str>) -> bool {
        if mount.len() != path.len() {
            return false;
        }
        
        mount.iter().zip(path.iter()).all(|(a,b)| a==b)
    }

    fn starts_with<'a>(mount: &Vec<String>, path: &Vec<&'a str>) -> Option<Vec<&'a str>> {
        if mount.len() > path.len() {
            return None
        }
        if mount.len() == 0 {
            return Some(path.clone())
        }

        let mut mount_iter =  mount.iter();
        let empty_string = "".to_string();
        let end: Vec<&str> = path.iter()
            .zip(iter::repeat_with(|| mount_iter.next().unwrap_or(&empty_string)))
            .skip_while(|(a,b)| a == b)
            .map(|(a, _)| *a)
            .collect();
        
        if path.len() - end.len() == mount.len() {
            Some(end)
        } else {
            None
        }
    }

    pub fn mount(&mut self, path: &str, config: Configuration) -> Option<()> {
        let path = split_path(path);
        for (m_path, _) in self.mounts.iter() {
            if Mount::eq(m_path, &path) {
                return None
            }
        }

        self.mounts.push((path.iter().map(|s| s.to_string()).collect(), config));
        Some(())
    }

    pub fn unmount(&mut self, path: &str) {
        let path = split_path(path);
        let mut to_remove = None;
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if Mount::eq(m_path, &path) {
                to_remove = Some(i);
                break;
            }
        }

        if let Some(i) = to_remove {
            self.mounts.remove(i);
        }
    }

    pub(crate) fn resolve_pair<'a>(&self, path_a: &'a str, path_b: &'a str) -> Option<(PathPair, Vec<&'a str>, Vec<&'a str>)> {
        let mut resolved_a: Option<(usize, Vec<&str>)> = None;
        let mut resolved_b: Option<(usize, Vec<&str>)> = None;
        let path_a = split_path(path_a);
        let path_b = split_path(path_b);
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if let Some(end) = Mount::starts_with(m_path, &path_a) {
                if let Some((_, last)) = &resolved_a {
                    if last.len() > end.len() {
                        resolved_a = Some((i, end))
                    }
                } else {
                    resolved_a = Some((i, end))
                }
            }
            if let Some(end) = Mount::starts_with(m_path, &path_b) {
                if let Some((_, last)) = &resolved_b {
                    if last.len() > end.len() {
                        resolved_b = Some((i, end))
                    }
                } else {
                    resolved_b = Some((i, end))
                }
            }
        }

        let (a, end_a) = if let Some((i, end)) = resolved_a {
            (i, end)
        } else {
            return None
        };
        let (b,  end_b) = if let Some((i, end)) = resolved_b {
            (i, end)
        } else {
            return None
        };
        
        if a == b {
            let (_, config) = &self.mounts[a];
            Some((PathPair::Single(config.clone()), end_a, end_b))
        } else {
            let (_, config_a) = &self.mounts[a];
            let (_, config_b) = &self.mounts[b];
            Some((PathPair::Pair(config_a.clone(), config_b.clone()), end_a, end_b))
        }
    }

    pub fn resolve<'a>(&self, path: &'a str) -> Option<(Configuration, Vec<&'a str>)> {
        let path= split_path(path);
        let mut resolved: Option<(usize, Vec<&str>)> = None;
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if let Some(end) = Mount::starts_with(m_path, &path) {
                if let Some((_, last)) = &resolved {
                    if last.len() > end.len()  {
                        resolved = Some((i, end))
                    }
                } else {
                    resolved = Some((i, end.clone()))
                }
            }
        }

        if let Some((i, end)) = resolved {
            let (_, config) = &self.mounts[i];
            Some((config.clone(), end))
        } else {
            None
        }
    }
}


#[cfg(test)]
mod tests {
    use fuse3::{async_trait, Result, Errno};

    use crate::config::{ConfigHooks, EntryType};

    use super::*;

    struct EmptyConfig;

    impl EmptyConfig {
        pub fn new() -> Configuration {
            Arc::new(RwLock::new(EmptyConfig))
        }
    }

    #[async_trait]
    impl ConfigHooks for EmptyConfig {
        async fn entires(&self, _parent: &Vec<&str>) -> Result<Vec<&str>> {
            Err(Errno::new_not_exist())
        }
        async fn lookup(&self, _parent: &Vec<&str>, _name: &str) -> Result<(EntryType, u64)>{
            Err(Errno::new_not_exist())
        }
        async fn lookup_path(&self, _path: &Vec<&str>) -> Result<(EntryType, u64)> {
            Err(Errno::new_not_exist())
        }
        async fn contains(&self, _parent: &Vec<&str>, _name: &str) -> bool {
            false
        }
    
        async fn mk_data(&mut self, _parent: &Vec<&str>, _name: &str) -> Result<()>{
            Err(Errno::new_not_exist())
        }
        async fn mk_obj(&mut self, _parent: &Vec<&str>, _name: &str) -> Result<()>{
            Err(Errno::new_not_exist())
        }
        async fn mv(&mut self, _parent: &Vec<&str>, _new_parent: &Vec<&str>, _name: &str, _new_name: &str) -> Result<()>{
            Err(Errno::new_not_exist())
        }
        async fn rm(&mut self, _parent: &Vec<&str>, _name: &str) -> Result<()>{
            Err(Errno::new_not_exist())
        }
        async fn rn(&mut self, _parent: &Vec<&str>, _name: &str, _new_name: &str) -> Result<()>{
            Err(Errno::new_not_exist())
        }
    
        async fn fetch(&mut self, _data_node: &Vec<&str>) -> Result<Vec<u8>>{
            Err(Errno::new_not_exist())
        }
        async fn size(&mut self, _data_node: &Vec<&str>) -> Result<u64>{
            Err(Errno::new_not_exist())
        }
        async fn update(&mut self, _data_node: &Vec<&str>, _data: Vec<u8>) -> Result<()>{
            Err(Errno::new_not_exist())
        }
    }

    #[tokio::test]
    async fn test() {
        let mount = Mount::new(); 

        let mut mount = mount.write().await;

        mount.mount("/a/", EmptyConfig::new());
        mount.mount("/b/", EmptyConfig::new());
        mount.mount("/b/a", EmptyConfig::new());
        mount.mount("/b/b", EmptyConfig::new());
        mount.mount("/", EmptyConfig::new());

        assert_eq!(mount.resolve("/a").unwrap().1.join("/"), "");
        assert_eq!(mount.resolve("/a/file").unwrap().1.join("/"), "file");
        assert_eq!(mount.resolve("/a/dir/file").unwrap().1.join("/"), "dir/file");


        assert_eq!(mount.resolve("/b").unwrap().1.join("/"), "");
        assert_eq!(mount.resolve("/b/file").unwrap().1.join("/"), "file");
        assert_eq!(mount.resolve("/b/dir/file").unwrap().1.join("/"), "dir/file");


        assert_eq!(mount.resolve("/b/a").unwrap().1.join("/"), "");
        assert_eq!(mount.resolve("/b/a/file").unwrap().1.join("/"), "file");
        assert_eq!(mount.resolve("/b/a/dir/file").unwrap().1.join("/"), "dir/file");


        assert_eq!(mount.resolve("/b/b").unwrap().1.join("/"), "");
        assert_eq!(mount.resolve("/b/b/file").unwrap().1.join("/"), "file");
        assert_eq!(mount.resolve("/b/b/dir/file").unwrap().1.join("/"), "dir/file");

        let (path_pair, path_a, path_b) = mount.resolve_pair("/b/file1", "/b/file2").unwrap();
        assert!(matches!(path_pair, PathPair::Single(_)));
        assert_eq!(path_a.join("/"), "file1");
        assert_eq!(path_b.join("/"), "file2");


        let (path_pair, path_a, path_b) = mount.resolve_pair("/a/file1", "/b/a/file2").unwrap();
        assert!(matches!(path_pair, PathPair::Pair(_,_)));
        assert_eq!(path_a.join("/"), "file1");
        assert_eq!(path_b.join("/"), "file2");


        mount.unmount("/b/a");
        assert_eq!(mount.resolve("/b/a").unwrap().1.join("/"), "a");

        mount.unmount("/b");
        assert_eq!(mount.resolve("/b/b").unwrap().1.join("/"), "");
        assert_eq!(mount.resolve("/b/a").unwrap().1.join("/"), "b/a");

        mount.unmount("/");
        assert!(mount.resolve("/b/a").is_none());

    }
}