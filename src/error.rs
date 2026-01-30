use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum AppError {
    #[snafu(display("IO error: {}", source))]
    Io { source: std::io::Error },

    #[snafu(display("Glob pattern error: {}", source))]
    Glob { source: glob::PatternError },

    #[snafu(display("Volume error for '{}': {}", drive, source))]
    VolumeOpen {
        drive: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to query USN journal: {}", source))]
    UsnJournalQuery { source: std::io::Error },

    #[snafu(display("Everything search error: {}", message))]
    Everything { message: String },

    #[snafu(display("Other error: {}", message))]
    Other { message: String },

    #[snafu(display("Lock poison error: {}", message))]
    LockPoison { message: String },
}

pub type Result<T> = std::result::Result<T, AppError>;
