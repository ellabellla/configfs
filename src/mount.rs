use std::{sync::Arc, iter, collections::HashSet, time::Duration};

use tokio::{sync::RwLock, task::JoinHandle, time};

use crate::{config::{Configuration, EntryType}, fs::split_path};


pub type FSMount = Arc<RwLock<Mount>>;

pub(crate) enum PathPair {
    Single(Configuration),
    Pair(Configuration, Configuration)
}

pub struct Mount {
    mounts: Vec<(Vec<String>, Configuration)>,
    configs: Option<HashSet<Configuration>>,
}

impl Mount {
    pub fn new() -> FSMount {
        Arc::new(RwLock::new(Mount{mounts: Vec::new(),  configs: Some(HashSet::new())}))
    }

    fn eq(mount: &Vec<String>, path: &Vec<&str>) -> bool {
        if mount.len() != path.len() {
            return false;
        }
        
        mount.iter().zip(path.iter()).all(|(a,b)| a==b)
    }

    fn starts_with<A, B>(mount: &Vec<A>, path: &Vec<B>) -> Option<Vec<B>> 
    where
        A: Default,
        B: std::cmp::PartialEq<A> + Clone + Copy
    {
        if mount.len() > path.len() {
            return None
        }
        if mount.len() == 0 {
            return Some(path.clone())
        }

        let mut mount_iter =  mount.iter();
        let empty = A::default();
        let end: Vec<B> = path.iter()
            .zip(iter::repeat_with(|| mount_iter.next().unwrap_or(&empty)))
            .skip_while(|(a,b)| **a == **b)
            .map(|(a, _)| *a)
            .collect();
        
        if path.len() - end.len() == mount.len() {
            Some(end)
        } else {
            None
        }
    }

    pub async fn spawn_tickers(&mut self) -> Vec<JoinHandle<()>> {
        let Some(configs) = &self.configs else {
            return vec![]
        };
        let mut join_handles = Vec::with_capacity(configs.len());
        for config in configs {
            let interval = config.tick_interval().await;
            if interval.as_nanos() != 0 {
                join_handles.push(self.spawn_ticker(config, interval));
            }
        }
        self.configs = None;
        join_handles
    }

    fn spawn_ticker(&self, config: &Configuration, interval: Duration) -> JoinHandle<()> {
        match config {
            Configuration::Basic(config) => {
                let config = config.clone();
                tokio::spawn(async move {
                    let mut interval = time::interval(interval);
            
                    loop {
                        interval.tick().await;
                        let mut config = config.write().await;
                        config.tick().await;
                        drop(config)
                    }
                })
            },
            Configuration::Complex(config) => {
                let config = config.clone();
                tokio::spawn(async move {
                    let mut interval = time::interval(interval);
            
                    loop {
                        interval.tick().await;
                        let mut config = config.write().await;
                        config.tick().await;
                        drop(config)
                    }
                })
            },
        }
    }

    pub fn mount(&mut self, path: &str, config: Configuration) -> Option<()> {
        let path = split_path(path);
        for (m_path, _) in self.mounts.iter() {
            if Mount::eq(m_path, &path) {
                return None
            }
        }
        self.mounts.push((path.iter().map(|s| s.to_string()).collect(), config.clone()));
        self.configs.as_mut().map(|c| c.insert(config.clone()));
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
            let config = &self.mounts.remove(i).1.clone();
            self.configs.as_mut().map(|c| c.remove(&config));
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

    pub fn entries(&self, path: &str) -> Option<Vec<(String, EntryType)>> {
        let path= split_path(path);
        let mut entries = HashSet::new();
        for (m_path, config) in self.mounts.iter() {
            if let Some(end) = Mount::starts_with(&path, &m_path.iter().map(|s| s.as_ref()).collect::<Vec<&str>>()) {
                let Some(entry) = end.first() else {
                    continue;
                };

                let entry_type = if end.len() == 1 {
                    EntryType::from(config)
                } else {
                    EntryType::Object
                };

                entries.insert((entry.to_string(), entry_type));
            }
        }
        if entries.is_empty() {
            None
        } else {
            Some(entries.into_iter().collect())
        }
    }

    pub fn contains(&self, path: & str) -> bool {
        let path= split_path(path);
        for (m_path, _) in self.mounts.iter() {
            if let Some(_) = Mount::starts_with(&path, &m_path.iter().map(|s| s.as_ref()).collect::<Vec<&str>>()) {
                return true
            }
        }

        false
    }
}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fuse3::{async_trait, Result, Errno};

    use crate::config::{ComplexConfigHook, EntryType};

    use super::*;

    struct EmptyConfig;

    impl EmptyConfig {
        pub fn new() -> Configuration {
            Configuration::Complex(Arc::new(RwLock::new(EmptyConfig)))
        }
    }

    #[async_trait]
    impl ComplexConfigHook for EmptyConfig {
        async fn entires(&self, _parent: &Vec<&str>) -> Result<Vec<String>> {
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

        async fn tick(&mut self) {
            
        }
        fn tick_interval(&self) -> Duration {
            Duration::from_secs(0)
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

        let sort = |mut v: Vec<(String, EntryType)>| { v.sort(); v };

        assert_eq!(sort(mount.entries("/").unwrap()), vec![("a".to_string(), EntryType::Object), ("b".to_string(), EntryType::Object)]);
        assert!(mount.entries("/a").is_none());
        assert_eq!(sort(mount.entries("/b").unwrap()), vec![("a".to_string(), EntryType::Object), ("b".to_string(), EntryType::Object)]);

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

        assert_eq!(mount.entries("/").unwrap(), vec![("a".to_string(), EntryType::Object), ("b".to_string(), EntryType::Object)]);
        
        mount.unmount("/");
        assert!(mount.resolve("/b/a").is_none());

        mount.unmount("/b/b");
        assert!(mount.resolve("/b/b").is_none());

        assert_eq!(mount.entries("/").unwrap(), vec![("a".to_string(), EntryType::Object)]);
    }
}