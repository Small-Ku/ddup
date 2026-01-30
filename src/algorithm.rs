use std::cmp::min;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek};
use std::ops::FnMut;
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

fn reduce_by_content<'a>(
    size: u64,
    paths: &[&'a Path],
    comparison: &Comparison,
) -> Vec<Vec<&'a Path>> {
    let mut map = HashMap::with_capacity(paths.len());

    // Group files by crc32 of beginning
    for path in paths {
        let mut file = match fs::File::open(path) {
            Ok(f) => f,
            _ => continue,
        };

        let hash_result = match comparison {
            Comparison::Fuzzy => calculate_fuzzy_hash(size, &mut file),
            Comparison::Strict => calculate_hash(&mut file),
        };

        if let Ok(hash) = hash_result {
            map.entry(hash).or_insert(Vec::new()).push(*path);
        }
    }

    // Filter out single occurrences
    map.retain(|_, v| v.len() > 1);

    map.values().cloned().collect()
}

pub fn run<P>(drive: &str, mut filter: P, comparison: Comparison) -> io::Result<Vec<DuplicateGroup>>
where
    P: FnMut(&&PathBuf) -> bool,
{
    let instant = Instant::now();

    println!("[1/3] Generating recursive dirlist");

    let dirlist = DirList::new(drive)?;
    let paths: Vec<&PathBuf> = dirlist.iter().filter(&mut filter).collect();

    println!("Finished in {} seconds", instant.elapsed().as_secs_f32());

    let instant = Instant::now();

    println!("[2/3] Grouping by file size");

    // Group files by size
    let mut map: HashMap<u64, Vec<&Path>> = HashMap::with_capacity(paths.len());
    let progress = ProgressBar::new(paths.len() as u64);

    for path in paths.into_iter() {
        progress.inc(1);
        let file_size = match fs::metadata(path) {
            Ok(m) => m.len(),
            _ => continue,
        };

        map.entry(file_size).or_default().push(path);
    }
    progress.finish();

    // Filter out single occurrences
    map.retain(|_, v| v.len() > 1);

    println!("Finished in {} seconds", instant.elapsed().as_secs_f32());

    let instant = Instant::now();

    println!("[3/3] Grouping by hash in thread pool");

    // Print all duplicates and collect them
    let duplicates = Mutex::new(Vec::new());
    let keys: Vec<u64> = map.keys().cloned().collect();
    // Iterate through size groups simultaneously
    keys.par_iter().for_each(|size: &u64| {
        let same_size_paths = &map[size];
        for same_crc_paths in reduce_by_content(*size, same_size_paths, &comparison).into_iter() {
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

            println!("Potential duplicates [{} bytes]", size);
            for path in &paths {
                println!("\t{}", path);
            }
        }
    });

    println!("Finished in {} seconds", instant.elapsed().as_secs_f32());
    Ok(duplicates.into_inner().unwrap())
}
