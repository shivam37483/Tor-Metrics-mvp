//! Parsing Tor consensus documents into metrics.

use crate::fetch::ConsensusFile; // Import ConsensusFile from fetch.rs
use std::collections::HashMap;
use std::error::Error;

/// Parses consensus documents into a structured format.
///
/// For each consensus file, this function extracts the number of relays by counting
/// lines that start with "r ". The result is a map where each key is the file path,
/// and the value is the relay count for that file.
///
/// # Arguments
/// - `consensus_files`: A vector of `ConsensusFile` structs containing the file path and content.
///
/// # Returns
/// - `Ok(HashMap<String, u64>)`: A map of file paths to their respective relay counts.
/// - `Err(Box<dyn Error>)`: An error if parsing fails (currently not expected in the MVP).
///
/// # Examples
/// ```
/// use tor_metrics_mvp::fetch::ConsensusFile;
/// use tor_metrics_mvp::parse::parse_consensuses;
/// let files = vec![ConsensusFile {
///   path: "file1".to_string(),
///   last_modified: 0,
///   content: "r relay1...\nr relay2...\n".to_string(),
/// }];
/// let metrics = parse_consensuses(files).unwrap();
/// assert_eq!(metrics["file1"], 2);
/// ```
pub fn parse_consensuses(consensus_files: Vec<ConsensusFile>) -> Result<HashMap<String, u64>, Box<dyn Error>> {
  let mut metrics = HashMap::new();

  for file in consensus_files {
    let relay_count = file.content.lines().filter(|line| line.starts_with("r ")).count() as u64;
    metrics.insert(file.path, relay_count);
  }

  Ok(metrics)
}