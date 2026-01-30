use crate::error::Result;
use std::cmp::min;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use crc::{Crc, CRC_32_ISO_HDLC};

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

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

fn calculate_fuzzy_hash(size: u64, file: &mut fs::File) -> io::Result<u32> {
    let mut digest = CRC.digest();
    let mut buffer = [0u8; 1024 * 4];
    let mut offset: u64 = 0;

    // Digest with exponentially decreasing density
    while offset + (buffer.len() as u64) < size {
        file.seek(io::SeekFrom::Start(offset))?;
        let bytes_read = file.read(&mut buffer)? as u64;
        if bytes_read == 0 {
            break;
        }
        digest.update(&buffer[..bytes_read as usize]);
        offset += bytes_read;
        offset *= 2;
    }

    // Digest the last chunk
    let read_size = min(size, buffer.len() as u64) as usize;
    if read_size > 0 {
        let offset_from_end = -(read_size as i64);
        file.seek(io::SeekFrom::End(offset_from_end))?;
        file.read_exact(&mut buffer[..read_size])?;
        digest.update(&buffer[..read_size]);
    }

    Ok(digest.finalize())
}

// @TODO: Replace this with sha512
fn calculate_hash(file: &mut fs::File) -> io::Result<u32> {
    let mut digest = CRC.digest();
    let mut buffer = [0u8; 1024 * 4];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        digest.update(&buffer[..bytes_read]);
    }

    Ok(digest.finalize())
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
            let mut reduced_map: HashMap<u32, Vec<&Path>> = HashMap::new();

            // Collect hashes in parallel
            let hashes: Vec<Option<(u32, &Path)>> = same_size_paths
                .par_iter()
                .map(|path| {
                    let mut file = match fs::File::open(path) {
                        Ok(f) => f,
                        _ => return None,
                    };

                    let hash_result = match comparison {
                        Comparison::Fuzzy => calculate_fuzzy_hash(*size, &mut file),
                        Comparison::Strict => calculate_hash(&mut file),
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

        for same_crc_paths in reduced_groups {
            let paths: Vec<String> = same_crc_paths
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
