use crate::error::Result;
use std::cmp::min;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use rapidhash::fast::RapidHasher;
use std::hash::Hasher;

use indicatif::ProgressBar;
use nanoserde::SerJson;
use rayon::prelude::*;

use super::DirList;

#[derive(SerJson, Debug, Clone)]
pub struct DuplicateGroup {
    pub size: u64,
    pub paths: Vec<String>,
}

#[derive(Debug)]
pub enum Comparison {
    Fuzzy,
    Strict,
}

fn calculate_fuzzy_hash(size: u64, path: &Path) -> io::Result<u64> {
    if size == 0 {
        return Ok(0);
    }

    let file = fs::File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mut hasher = RapidHasher::default();
    let mut offset: u64 = 0;
    let chunk_size: u64 = 4096;

    // Digest with exponentially decreasing density
    while offset + chunk_size < size {
        let chunk = &mmap[offset as usize..(offset + chunk_size) as usize];
        hasher.write(chunk);
        offset += chunk_size;
        offset *= 2;
    }

    // Digest the last chunk
    let read_size = min(size, chunk_size) as usize;
    if read_size > 0 {
        let start = (size as usize).saturating_sub(read_size);
        let chunk = &mmap[start..size as usize];
        hasher.write(chunk);
    }

    Ok(hasher.finish())
}

fn calculate_full_hash(path: &Path) -> io::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    Ok(hasher.finalize())
}

pub fn run(
    drive: &str,
    matcher: Option<&str>,
    options: glob::MatchOptions,
    comparison: Comparison,
    backend: crate::dirlist::Backend,
) -> Result<Vec<DuplicateGroup>> {
    let instant = Instant::now();

    log::info!("[1/3] Generating recursive dirlist");

    let dirlist = DirList::new(drive, matcher, options, backend)?;

    log::info!("Finished in {} seconds", instant.elapsed().as_secs_f32());

    let instant = Instant::now();

    log::info!("[2/3] Grouping by file size");

    // Group files by size
    let entries: Vec<&(PathBuf, u64)> = dirlist.iter().collect();
    let mut map: HashMap<u64, Vec<&Path>> = HashMap::with_capacity(entries.len());
    let progress = ProgressBar::new(entries.len() as u64);

    for (path, file_size) in entries.into_iter() {
        progress.inc(1);
        map.entry(*file_size).or_default().push(path);
    }
    progress.finish();

    // Filter out single occurrences
    map.retain(|_, v| v.len() > 1);

    log::info!("Finished in {} seconds", instant.elapsed().as_secs_f32());

    let instant = Instant::now();

    log::info!("[3/3] Grouping by hash in thread pool");

    // Print all duplicates and collect them
    let duplicates = Mutex::new(Vec::new());
    let keys: Vec<u64> = map.keys().cloned().collect();

    let progress = ProgressBar::new(keys.len() as u64);

    // Iterate through size groups simultaneously
    keys.par_iter().for_each(|size: &u64| {
        progress.inc(1);
        let same_size_paths = &map[size];

        // Parallelize the hashing of files within the same size group
        let reduced_groups: Vec<Vec<&Path>> = if same_size_paths.len() > 1 {
            // Group by hash locally
            let mut reduced_map: HashMap<String, Vec<&Path>> = HashMap::new();

            // Collect hashes in parallel
            let hashes: Vec<Option<(String, &Path)>> = same_size_paths
                .par_iter()
                .map(|path| {
                    let hash_result = match comparison {
                        Comparison::Fuzzy => {
                            calculate_fuzzy_hash(*size, path).map(|h| h.to_string())
                        }
                        Comparison::Strict => calculate_full_hash(path).map(|h| h.to_string()),
                    };

                    hash_result.ok().map(|hash| (hash, *path))
                })
                .collect();

            // Group by hash locally (sequential aggregation is fast enough for reduced set)
            for (hash, path) in hashes.into_iter().flatten() {
                reduced_map.entry(hash).or_default().push(path);
            }

            reduced_map.retain(|_, v| v.len() > 1);
            reduced_map.into_values().collect()
        } else {
            Vec::new()
        };

        for same_hash_paths in reduced_groups {
            let paths: Vec<String> = same_hash_paths
                .into_iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect();

            {
                let mut guard = duplicates.lock().unwrap();
                guard.push(DuplicateGroup {
                    size: *size,
                    paths: paths.clone(),
                });
            }
        }
    });

    progress.finish();

    log::info!("Finished in {} seconds", instant.elapsed().as_secs_f32());
    duplicates
        .into_inner()
        .map_err(|_| crate::error::AppError::LockPoison {
            message: "Duplicate groups mutex was poisoned".to_string(),
        })
}
