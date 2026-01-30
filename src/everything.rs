use everything3_sys::*;
use std::ffi::CString;
use std::path::PathBuf;
use std::ptr;

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
        let mut results_vec = Vec::new();
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

            // Match path is important for drive-based searches
            Everything3_SetSearchMatchPath(search_state, 1);
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
            results_vec.reserve(count as usize);

            let mut buffer = [0u8; 4096];
            let mut skipped_dirs = 0u64;
            let mut zero_len_paths = 0u64;
            let mut added_files = 0u64;
            let mut skipped_hardlinks = 0u64;

            for i in 0..count {
                // Skip directories (FILE_ATTRIBUTE_DIRECTORY = 0x10)
                let attributes = Everything3_GetResultAttributes(results, i);
                if (attributes & 0x00000010) != 0 {
                    skipped_dirs += 1;
                    continue;
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
                                    skipped_hardlinks += 1;
                                    continue;
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
                        results_vec.push((PathBuf::from(path_str), size));
                        added_files += 1;
                    } else {
                        zero_len_paths += 1;
                    }
                } else {
                    let path_str = std::str::from_utf8(&buffer[..len as usize]).unwrap_or("");
                    let size = Everything3_GetResultSize(results, i);
                    results_vec.push((PathBuf::from(path_str), size));
                    added_files += 1;
                }
            }

            eprintln!(
                "[Everything] Debug: Processed {} results - {} dirs skipped, {} zero-length paths, {} hardlinks skipped, {} files added",
                count, skipped_dirs, zero_len_paths, skipped_hardlinks, added_files
            );

            Everything3_DestroyResultList(results);
            Everything3_DestroySearchState(search_state);
        }
        results_vec
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
