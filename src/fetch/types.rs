use std::fmt::Debug;

/// Represents a fetched bridge pool assignment file's metadata and content.
///
/// This struct encapsulates the path, last-modified timestamp, and content of a bridge pool
/// assignment file, making it suitable for parsing or database export. It stores both the 
/// text content as a String and the raw bytes for digest calculation.
#[derive(Debug)]
pub struct BridgePoolFile {
    /// Relative path of the file (e.g., "bridge_pool_assignments/2022-04-09-00-29-37").
    pub path: String,
    /// Last modified timestamp in milliseconds since the Unix epoch.
    pub last_modified: i64,
    /// Raw textual content of the file.
    pub content: String,
    /// Raw bytes content of the file for SHA-256 digest calculation.
    pub raw_content: Vec<u8>,
} 