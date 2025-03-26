use std::collections::BTreeMap;
use std::fmt::Debug;

/// Represents a parsed bridge pool assignment, containing the publication timestamp and a map of bridge entries.
/// 
/// This struct stores both the structured data extracted from the file and the raw bytes needed for
/// digest calculation according to the original metrics library approach.
#[derive(Debug)]
pub struct ParsedBridgePoolAssignment {
    /// The time in milliseconds since the epoch when this descriptor was published.
    pub published_millis: i64,
    /// A map of bridge fingerprints (SHA-1 digests as 40-character hex strings) to their assignment strings.
    pub entries: BTreeMap<String, String>,
    /// Raw content of the file for file digest calculation using SHA-256.
    pub raw_content: Vec<u8>,
    /// Map of fingerprints to raw line bytes for individual assignment digest calculation using SHA-256.
    /// Each line's bytes are used to generate a unique digest for database storage.
    pub raw_lines: BTreeMap<String, Vec<u8>>,
} 