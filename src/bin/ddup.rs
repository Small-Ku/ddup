use std::time::Instant;

use clap::{Arg, ArgAction, ArgMatches, Command};

use glob::MatchOptions;

use ddup::algorithm::{self, Comparison};
use nanoserde::SerJson;
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
                .required(true)
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

    let drive = args
        .get_one::<String>("drive")
        .expect("Drive format is `<letter>:`");

    let instant = Instant::now();

    // Determine the comparison method
    let comparison = match args.get_flag("strict") {
        true => Comparison::Strict,
        false => Comparison::Fuzzy,
    };

    // Determine the backend preference
    let backend = if args.get_flag("everything") {
        ddup::Backend::Everything
    } else {
        ddup::Backend::USN
    };

    let result = if let Some(pattern) = args.get_one::<String>("match") {
        let is_sensitive = !args.get_flag("i");
        log::info!(
            "Scanning drive {} with matcher `{}` ({}) [{:?} comparison, preference: {:?}]",
            drive,
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

        algorithm::run(drive, Some(pattern), options, comparison, backend)
    } else {
        log::info!(
            "Scanning drive {} [{:?} comparison, preference: {:?}]",
            drive,
            comparison,
            backend
        );
        let options = MatchOptions {
            case_sensitive: false,
            require_literal_leading_dot: false,
            require_literal_separator: false,
        };
        algorithm::run(drive, None, options, comparison, backend)
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
