use std::sync::Arc;

use tokio::sync::RwLock;

use crate::config::Configuration;


pub type FSMount = Arc<RwLock<Mount>>;

pub(crate) enum PathPair {
    Single(Configuration),
    Pair(Configuration, Configuration)
}

pub struct Mount {
    mounts: Vec<(String, Configuration)>,
}

impl Mount {
    pub fn mount(&mut self, path: &str, config: Configuration) -> Option<()> {
        for (m_path, _) in self.mounts.iter() {
            if m_path == path {
                return None
            }
        }

        self.mounts.push((path.to_string(), config));
        Some(())
    }

    pub fn unmount(&mut self, path: &str) {
        let mut to_remove = None;
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if m_path == path {
                to_remove = Some(i);
                break;
            }
        }

        if let Some(i) = to_remove {
            self.mounts.remove(i);
        }
    }

    pub(crate) fn resolve_pair<'a>(&self, path_a: &'a str, path_b: &'a str) -> Option<(PathPair, &'a str, &'a str)> {
        let mut resolved_a = None;
        let mut resolved_b = None;
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if path_a.starts_with(m_path) {
                if let Some((i, len)) = resolved_a {
                    let new_len =  path_a.len() - m_path.len();
                    if len < new_len {
                        resolved_a = Some((i, new_len))
                    }
                } else {
                    resolved_a = Some((i, path_a.len() - m_path.len()))
                }
                break;
            }
            if path_b.starts_with(m_path) {
                if let Some((i, len)) = resolved_b {
                    let new_len =  path_b.len() - m_path.len();
                    if len < new_len {
                        resolved_b = Some((i, new_len))
                    }
                } else {
                    resolved_b = Some((i, path_b.len() - m_path.len()))
                }
                break;
            }
        }

        let a = if let Some((i, _)) = resolved_a {
            i
        } else {
            return None
        };
        let b = if let Some((i, _)) = resolved_b {
            i
        } else {
            return None
        };
        
        if a == b {
            let (path, config) = &self.mounts[a];
            Some((PathPair::Single(config.clone()), path_a.strip_prefix(path)?, path_b.strip_prefix(path)?))
        } else {
            let (m_path_a, config_a) = &self.mounts[a];
            let (m_path_b, config_b) = &self.mounts[b];
            Some((PathPair::Pair(config_a.clone(), config_b.clone()), path_a.strip_prefix(m_path_a)?, path_b.strip_prefix(m_path_b)?))
        }
    }

    pub fn resolve<'a>(&self, path: &'a str) -> Option<(Configuration, &'a str)> {
        let mut resolved = None;
        for (i, (m_path, _)) in self.mounts.iter().enumerate() {
            if path.starts_with(m_path) {
                if let Some((i, len)) = resolved {
                    let new_len =  path.len() - m_path.len();
                    if len < new_len {
                        resolved = Some((i, new_len))
                    }
                } else {
                    resolved = Some((i, path.len() - m_path.len()))
                }
                break;
            }
        }

        if let Some((i, _)) = resolved {
            let (m_path, config) = &self.mounts[i];
            Some((config.clone(), path.strip_prefix(m_path)?))
        } else {
            None
        }
    }
}