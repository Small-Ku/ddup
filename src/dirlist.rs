use crate::error::Result;
use indicatif::ProgressBar;
use rayon::prelude::*;
use snafu::ResultExt;
use std::path::{Path, PathBuf};

use super::utils::{hash_map_to_paths, usn_records_to_hash_map};
use super::Ntfs;
use super::UsnRange;
use super::Volume;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Everything,
    USN,
}

pub struct DirList {
    entries: Vec<(PathBuf, u64)>,
}

impl DirList {
    pub fn new(
        drive: &str,
        matcher: Option<&str>,
        options: glob::MatchOptions,
        backend: Backend,
    ) -> Result<Self> {
        match backend {
            Backend::Everything => {
                if let Some(everything) = super::everything::EverythingSearch::new() {
                    // Combine drive and matcher for Everything search
                    let mut query = drive.to_string();
                    if !query.ends_with('\\') {
                        query.push('\\');
                    }
                    if let Some(m) = matcher {
                        query.push_str(" \"");
                        query.push_str(m);
                        query.push('"');
                    }

                    match everything.get_all_files(&query, options.case_sensitive) {
                        Ok(entries) => {
                            if !entries.is_empty() {
                                return Ok(DirList { entries });
                            }
                            log::warn!(
                                "[Everything] Warning: Search returned no results, falling back to USN"
                            );
                        }
                        Err(e) => {
                            log::warn!("[Everything] Error: {}, falling back to USN", e);
                        }
                    }
                } else {
                    log::warn!("[Everything] Warning: Service not found, falling back to USN");
                }
                // Fallback to USN
                Self::new(drive, matcher, options, Backend::USN)
            }
            Backend::USN => {
                let volume = Volume::open(&(String::from(r"\\.\") + drive))
                    .context(crate::error::VolumeOpenSnafu { drive })?;
                let journal = volume
                    .query_usn_journal()
                    .context(crate::error::UsnJournalQuerySnafu)?;
                let range = UsnRange {
                    low: journal.LowestValidUsn,
                    high: journal.NextUsn,
                };
                let usn_records = volume.usn_records(&range);
                let map = usn_records_to_hash_map(usn_records);
                let paths = hash_map_to_paths(&map);

                let pattern =
                    matcher.map(|m| glob::Pattern::new(m).context(crate::error::GlobSnafu));
                let pattern = match pattern {
                    Some(Ok(p)) => Some(p),
                    Some(Err(e)) => return Err(e),
                    None => None,
                };

                log::info!("Processing {} paths from USN journal", paths.len());
                let progress = ProgressBar::new(paths.len() as u64);
                let entries: Vec<_> = paths
                    .par_iter()
                    .map(|p| {
                        progress.inc(1);
                        Path::new(drive).join(p)
                    })
                    .filter(|full_path| {
                        pattern
                            .as_ref()
                            .is_none_or(|pat| pat.matches_path_with(full_path, options))
                    })
                    .filter_map(|full_path| {
                        std::fs::metadata(&full_path)
                            .ok()
                            .filter(|m| m.is_file())
                            .map(|m| (full_path, m.len()))
                    })
                    .collect();
                progress.finish();

                Ok(DirList { entries })
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(PathBuf, u64)> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::Instant;
    use walkdir;

    #[test]
    fn compare_walkdir_to_dirlist() {
        println!("What is this\r\n");
        let instant = Instant::now();
        let mut v1 = Vec::new();
        for p in walkdir::WalkDir::new(r"C:\") {
            if let Ok(d) = p {
                if d.file_type().is_file() {
                    v1.push(String::from(d.path().to_str().unwrap()));
                }
            }
        }
        println!(
            "WalkDir got {} entries in {} seconds",
            v1.len(),
            instant.elapsed().as_secs_f32()
        );

        let instant = Instant::now();
        let mut v2 = Vec::new();
        let options = glob::MatchOptions {
            case_sensitive: false,
            require_literal_leading_dot: false,
            require_literal_separator: false,
        };
        let dirlist = DirList::new("C:", None, options, Backend::USN).unwrap();
        for (p, _) in dirlist.iter() {
            v2.push(String::from(p.to_str().unwrap()));
        }
        println!(
            "Dirlist got {} entries in {} seconds",
            v2.len(),
            instant.elapsed().as_secs_f32()
        );

        let set1: HashSet<String> = v1.iter().cloned().map(|s| s.to_lowercase()).collect();
        let set2: HashSet<String> = v2.iter().cloned().map(|s| s.to_lowercase()).collect();

        println!("a - b:");
        for diff in set1.difference(&set2).into_iter().take(100) {
            println!("\t{}", diff);
        }

        println!("b - a:");
        for diff in set2.difference(&set1).into_iter().take(10) {
            println!("\t{}", diff);
        }
    }
}
