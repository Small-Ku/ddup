use std::path::PathBuf;
use std::time::Instant;

use clap::{Arg, ArgAction, ArgMatches, Command};

use glob::{MatchOptions, Pattern};

use ddup::algorithm::{self, Comparison};

fn parse_args() -> ArgMatches {
    Command::new("ddup")
        .about("This tool identifies duplicated files in Windows NTFS Volumes")
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
        .get_matches()
}

fn main() {
    let args = parse_args();

    let drive = args
        .get_one::<String>("drive")
        .expect("Drive format is `<letter>:`");

    let instant = Instant::now();

    // Determine the comparison method
    let comparison = match args.get_flag("strict") {
        true => Comparison::Strict,
        false => Comparison::Fuzzy,
    };

    if let Some(pattern) = args.get_one::<String>("match") {
        let is_sensitive = !args.get_flag("i");
        println!(
            "Scanning drive {} with matcher `{}` ({}) [{:?} comparison]",
            drive,
            pattern,
            if is_sensitive {
                "case-sensitive"
            } else {
                "case-insensitive"
            },
            comparison
        );

        let options = MatchOptions {
            case_sensitive: is_sensitive,
            require_literal_leading_dot: false,
            require_literal_separator: false,
        };

        algorithm::run(
            drive,
            |path: &&PathBuf| {
                Pattern::new(pattern)
                    .expect("Illegal matcher syntax")
                    .matches_path_with(path.as_path(), options)
            },
            comparison,
        )
        .expect("Failed to run duplicate detection");
    } else {
        println!("Scanning drive {} [{:?} comparison]", drive, comparison);
        algorithm::run(drive, |_| true, comparison).expect("Failed to run duplicate detection");
    }

    println!(
        "Overall finished in {} seconds",
        instant.elapsed().as_secs_f32()
    );
}
