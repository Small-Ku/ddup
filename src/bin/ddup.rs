use std::time::Instant;

use clap::{Arg, ArgAction, ArgMatches, Command};

use glob::MatchOptions;

use ddup::algorithm::{self, Comparison};
use nanoserde::SerJson;
use rayon::prelude::*;
use std::fs;

fn parse_args() -> ArgMatches {
    Command::new("ddup")
        .about("This tool identifies duplicated files in Windows NTFS Volumes")
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose logging")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("drive")
                .help("The drive letter to scan (example `C:`)")
                .required_unless_present("wiztree")
                .index(1),
        )
        .arg(
            Arg::new("match")
                .short('m')
                .long("match")
                .value_name("PATTERN")
                .help("Scan only paths that match the glob pattern (example `**.dmp`)")
                .num_args(1),
        )
        .arg(
            Arg::new("i")
                .short('i')
                .help("Treat the matcher as case-insensitive")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("strict")
                .short('s')
                .long("strict")
                .help("Do not perform fuzzy hashing, guarantees equivalence")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("everything")
                .short('E')
                .long("everything")
                .help("Use Everything search backend (instead of default USN journal)")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("export")
                .short('e')
                .long("export")
                .value_name("FILE")
                .help("Export the duplicated file list to a JSON file")
                .num_args(1),
        )
        .arg(
            Arg::new("link")
                .short('l')
                .long("link")
                .help("Replace duplicates with hardlinks")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("wiztree")
                .short('w')
                .long("wiztree")
                .value_name("FILE")
                .help("Use a WizTree CSV file as the source")
                .num_args(1),
        )
        .get_matches()
}

fn main() {
    let args = parse_args();

    if args.get_flag("verbose") {
        std::env::set_var("RUST_LOG", "debug");
    } else {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let instant = Instant::now();

    // Determine the comparison method
    let comparison = if args.get_flag("strict") || args.get_flag("link") {
        if args.get_flag("link") && !args.get_flag("strict") {
            log::warn!("Hardlink option enabled: Forcing strict comparison to prevent data loss.");
        }
        Comparison::Strict
    } else {
        Comparison::Fuzzy
    };

    // Determine the backend preference
    let (backend, source) = if let Some(wiztree_path) = args.get_one::<String>("wiztree") {
        (ddup::Backend::WizTree, wiztree_path.as_str())
    } else if args.get_flag("everything") {
        (
            ddup::Backend::Everything,
            args.get_one::<String>("drive").unwrap().as_str(),
        )
    } else {
        (
            ddup::Backend::USN,
            args.get_one::<String>("drive").unwrap().as_str(),
        )
    };

    let result = if let Some(pattern) = args.get_one::<String>("match") {
        let is_sensitive = !args.get_flag("i");
        log::info!(
            "Scanning {} with matcher `{}` ({}) [{:?} comparison, preference: {:?}]",
            source,
            pattern,
            if is_sensitive {
                "case-sensitive"
            } else {
                "case-insensitive"
            },
            comparison,
            backend
        );

        let options = MatchOptions {
            case_sensitive: is_sensitive,
            require_literal_leading_dot: false,
            require_literal_separator: false,
        };

        algorithm::run(source, Some(pattern), options, comparison, backend)
    } else {
        log::info!(
            "Scanning {} [{:?} comparison, preference: {:?}]",
            source,
            comparison,
            backend
        );
        let options = MatchOptions {
            case_sensitive: false,
            require_literal_leading_dot: false,
            require_literal_separator: false,
        };
        algorithm::run(source, None, options, comparison, backend)
    };

    let duplicates = match result {
        Ok(d) => d,
        Err(e) => {
            log::error!("Failed to run duplicate detection: {}", e);
            std::process::exit(1);
        }
    };

    let export_path = args.get_one::<String>("export");
    if let Some(export_path) = export_path {
        let json = duplicates.serialize_json();
        fs::write(export_path, json).expect("Failed to write export file");
        log::info!("Exported {} groups to {}", duplicates.len(), export_path);
    }

    if args.get_flag("link") {
        let freed_space: u64 = duplicates
            .par_iter()
            .map(|group| {
                let mut group_freed = 0;
                if let Some(first) = group.paths.first() {
                    for path in &group.paths[1..] {
                        log::info!("Linking {} -> {}", path, first);
                        let tmp_path = format!("{}.ddup_tmp", path);

                        if let Err(e) = fs::rename(path, &tmp_path) {
                            log::error!("Failed to prepare link for {} (move failed): {}", path, e);
                            continue;
                        }

                        if let Err(e) = fs::hard_link(first, path) {
                            log::error!(
                                "Failed to link {} to {}: {}. Restoring original...",
                                path,
                                first,
                                e
                            );
                            if let Err(restore_e) = fs::rename(&tmp_path, path) {
                                log::error!(
                                    "CRITICAL: Failed to restore {} from backup: {}",
                                    path,
                                    restore_e
                                );
                            }
                        } else {
                            if let Err(e) = fs::remove_file(&tmp_path) {
                                log::warn!("Failed to remove backup file {}: {}", tmp_path, e);
                            } else {
                                group_freed += group.size;
                            }
                        }
                    }
                }
                group_freed
            })
            .sum();

        log::info!(
            "Deduplication complete. Estimated space freed: {} bytes",
            freed_space
        );
    }

    if export_path.is_none() || args.get_flag("verbose") {
        for group in &duplicates {
            println!("Potential duplicates [{} bytes]", group.size);
            for path in &group.paths {
                println!("\t{}", path);
            }
        }
    }

    log::info!(
        "Overall finished in {} seconds",
        instant.elapsed().as_secs_f32()
    );
}
