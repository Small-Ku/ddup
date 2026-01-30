use everything3_sys::*;
use rayon::prelude::*;
use std::ffi::CString;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

// Wrapper to allow passing raw pointers to rayon threads
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

pub struct EverythingSearch {
    client: *mut EVERYTHING3_CLIENT,
}

impl EverythingSearch {
    pub fn new() -> Option<Self> {
        unsafe {
            // First try default instance
            let mut client = Everything3_ConnectUTF8(ptr::null());
            let mut instance_used = "default";
            if client.is_null() {
                // Try 1.5a instance as fallback
                let instance_name = CString::new("1.5a").unwrap();
                client = Everything3_ConnectUTF8(instance_name.as_ptr() as *const u8);
                instance_used = "1.5a";
            }

            if client.is_null() {
                eprintln!("[Everything] Error: Could not connect to Everything service (ConnectUTF8 returned NULL for default and 1.5a instances)");
                None
            } else {
                eprintln!(
                    "[Everything] Debug: Connected to '{}' instance",
                    instance_used
                );
                Some(EverythingSearch { client })
            }
        }
    }

    pub fn get_all_files(&self, query_str: &str, case_sensitive: bool) -> Vec<(PathBuf, u64)> {
        let results_vec = Vec::new(); // Initial empty vec, will be replaced by collect
        unsafe {
            let search_state = Everything3_CreateSearchState();
            if search_state.is_null() {
                return results_vec;
            }

            // Request necessary properties
            Everything3_AddSearchPropertyRequest(search_state, EVERYTHING3_PROPERTY_ID_NAME);
            Everything3_AddSearchPropertyRequest(search_state, EVERYTHING3_PROPERTY_ID_PATH);
            Everything3_AddSearchPropertyRequest(search_state, EVERYTHING3_PROPERTY_ID_SIZE);
            Everything3_AddSearchPropertyRequest(search_state, EVERYTHING3_PROPERTY_ID_ATTRIBUTES);
            Everything3_AddSearchPropertyRequest(
                search_state,
                EVERYTHING3_PROPERTY_ID_PATH_AND_NAME,
            );
            // Request hardlink info for deduplication
            Everything3_AddSearchPropertyRequest(
                search_state,
                EVERYTHING3_PROPERTY_ID_HARD_LINK_COUNT,
            );
            Everything3_AddSearchPropertyRequest(
                search_state,
                EVERYTHING3_PROPERTY_ID_HARD_LINK_FILE_NAMES,
            );

            // Match path is important for drive-based searches
            Everything3_SetSearchMatchPath(search_state, 1);
            Everything3_SetSearchMatchCase(search_state, if case_sensitive { 1 } else { 0 });
            Everything3_SetSearchRequestTotalSize(search_state, 1);

            let query = CString::new(query_str).unwrap();
            Everything3_SetSearchTextUTF8(search_state, query.as_ptr() as *const u8);

            eprintln!(
                "[Everything] Debug: Executing search with query: {}",
                query_str
            );
            let results = Everything3_Search(self.client, search_state);

            if results.is_null() {
                let err = Everything3_GetLastError();
                eprintln!(
                    "[Everything] Error: Search for '{}' failed with error code {}",
                    query_str, err
                );
                Everything3_DestroySearchState(search_state);
                return results_vec;
            }

            let count = Everything3_GetResultListCount(results);
            if count == 0 {
                eprintln!(
                    "[Everything] Debug: Search for '{}' returned 0 results",
                    query_str
                );
            }

            let skipped_dirs = AtomicU64::new(0);
            let zero_len_paths = AtomicU64::new(0);
            let added_files = AtomicU64::new(0);
            let skipped_hardlinks = AtomicU64::new(0);

            // Wrap pointer for rayon
            let results_ptr = SendPtr(results);

            let collected_results: Vec<(PathBuf, u64)> = (0..count)
                .into_par_iter()
                .map(|i| {
                    let results = results_ptr.0;
                    let mut buffer = [0u8; 4096]; // Thread-local buffer

                     // Skip directories (FILE_ATTRIBUTE_DIRECTORY = 0x10)
                    let attributes = Everything3_GetResultAttributes(results, i);
                    if (attributes & 0x00000010) != 0 {
                        skipped_dirs.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }

                    // Check hardlinks
                    let hl_count = Everything3_GetResultPropertyDWORD(
                        results,
                        i,
                        EVERYTHING3_PROPERTY_ID_HARD_LINK_COUNT,
                    );
                    if hl_count > 1 {
                        // Get all hardlink names
                        let len_hl = Everything3_GetResultPropertyTextUTF8(
                            results,
                            i,
                            EVERYTHING3_PROPERTY_ID_HARD_LINK_FILE_NAMES,
                            buffer.as_mut_ptr(),
                            buffer.len() as u64,
                        );
                        if len_hl > 0 {
                            let hl_names_str =
                                std::str::from_utf8(&buffer[..len_hl as usize]).unwrap_or("");
                            let mut names: Vec<&str> = hl_names_str.split(';').collect();

                            let mut current_path_buffer = [0u8; 4096];
                            let len_cur = Everything3_GetResultFullPathNameUTF8(
                                results,
                                i,
                                current_path_buffer.as_mut_ptr(),
                                current_path_buffer.len() as u64,
                            );
                            if len_cur > 0 {
                                let current_path_full =
                                    std::str::from_utf8(&current_path_buffer[..len_cur as usize])
                                        .unwrap_or("");
                                // Strip drive letter "X:" if present
                                let current_path_suffix = if current_path_full.len() >= 2
                                    && current_path_full.chars().nth(1) == Some(':')
                                {
                                    &current_path_full[2..]
                                } else {
                                    current_path_full
                                };

                                names.sort();
                                if let Some(first) = names.first() {
                                    if *first != current_path_suffix {
                                        // We are not the leader, skip
                                        skipped_hardlinks.fetch_add(1, Ordering::Relaxed);
                                        return None;
                                    }
                                }
                            }
                        }
                    }

                    let len = Everything3_GetResultFullPathNameUTF8(
                        results,
                        i,
                        buffer.as_mut_ptr(),
                        buffer.len() as u64,
                    );

                    if len == 0 {
                        // Fallback to getting PATH_AND_NAME property directly if helper fails
                        let len2 = Everything3_GetResultPropertyTextUTF8(
                            results,
                            i,
                            EVERYTHING3_PROPERTY_ID_PATH_AND_NAME,
                            buffer.as_mut_ptr(),
                            buffer.len() as u64,
                        );
                        if len2 > 0 {
                            let path_str = std::str::from_utf8(&buffer[..len2 as usize]).unwrap_or("");
                            let size = Everything3_GetResultSize(results, i);
                            added_files.fetch_add(1, Ordering::Relaxed);
                            Some((PathBuf::from(path_str), size))
                        } else {
                            zero_len_paths.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    } else {
                        let path_str = std::str::from_utf8(&buffer[..len as usize]).unwrap_or("");
                        let size = Everything3_GetResultSize(results, i);
                        added_files.fetch_add(1, Ordering::Relaxed);
                        Some((PathBuf::from(path_str), size))
                    }
                })
                .flatten()
                .collect();

            eprintln!(
                "[Everything] Debug: Processed {} results - {} dirs skipped, {} zero-length paths, {} hardlinks skipped, {} files added",
                count, 
                skipped_dirs.load(Ordering::Relaxed), 
                zero_len_paths.load(Ordering::Relaxed), 
                skipped_hardlinks.load(Ordering::Relaxed), 
                added_files.load(Ordering::Relaxed)
            );

            Everything3_DestroyResultList(results);
            Everything3_DestroySearchState(search_state);
            
            collected_results
        }
    }
}

impl Drop for EverythingSearch {
    fn drop(&mut self) {
        unsafe {
            if !self.client.is_null() {
                Everything3_DestroyClient(self.client);
            }
        }
    }
}
