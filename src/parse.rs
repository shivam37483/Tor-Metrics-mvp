//! # Parsing Tor Bridge Pool Assignment Files into Structured Metrics
//!
//! This module provides functionality to parse bridge pool assignment files fetched from a CollecTor
//! instance into structured data. It processes the raw textual content, extracting publication
//! timestamps and bridge assignment entries, which are then encapsulated in
//! `ParsedBridgePoolAssignment` structs for further analysis or storage.
//!
//! ## Usage
//!
//! The main entry point is `parse_bridge_pool_files`, which accepts a vector of `BridgePoolFile`
//! structs and returns a vector of `ParsedBridgePoolAssignment` instances.
//!
//! ## Dependencies
//!
//! - `chrono`: For timestamp parsing and manipulation.
//! - `std::collections::BTreeMap`: For ordered storage of bridge entries.
//! - `anyhow`: For robust error handling.
//!
//! ## Error Handling
//!
//! Errors are handled with `anyhow::Result`, providing detailed context for parsing failures.

use crate::fetch::BridgePoolFile; // Import BridgePoolFile from fetch.rs
use anyhow::{Context, Result as AnyhowResult};
use chrono::NaiveDateTime;
use std::collections::BTreeMap;

/// Represents a parsed bridge pool assignment, containing the publication timestamp and a map of bridge entries.
#[derive(Debug)]
pub struct ParsedBridgePoolAssignment {
  /// The time in milliseconds since the epoch when this descriptor was published.
  pub published_millis: i64,
  /// A map of bridge fingerprints (SHA-1 digests as 40-character hex strings) to their assignment strings.
  pub entries: BTreeMap<String, String>,
}

/// Parses bridge pool assignment files into a structured format.
///
/// This function processes each provided `BridgePoolFile`, extracting the publication timestamp and
/// the map of bridge entries. It returns a vector of `ParsedBridgePoolAssignment` structs, each
/// corresponding to a parsed file.
///
/// # Arguments
///
/// * `bridge_pool_files` - A vector of `BridgePoolFile` structs containing the file path and content.
///
/// # Returns
///
/// * `Ok(Vec<ParsedBridgePoolAssignment>)` - A vector of parsed bridge pool assignments.
/// * `Err(anyhow::Error)` - An error if parsing fails for any file.
///
/// # Examples
///
/// ```rust
/// use tor_metrics_mvp::fetch::BridgePoolFile;
/// use tor_metrics_mvp::parse::parse_bridge_pool_files;
/// let files = vec![BridgePoolFile {
///   path: "file1".to_string(),
///   last_modified: 0,
///   content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".to_string(),
/// }];
/// let parsed = parse_bridge_pool_files(files).unwrap();
/// assert_eq!(parsed[0].published_millis, 1649464177000);
/// assert_eq!(parsed[0].entries["005fd4d7decbb250055b861579e6fdc79ad17bee"], "email transport=obfs4");
/// ```
pub fn parse_bridge_pool_files(
  bridge_pool_files: Vec<BridgePoolFile>,
) -> AnyhowResult<Vec<ParsedBridgePoolAssignment>> {
  let mut parsed_assignments = Vec::new();

  for file in bridge_pool_files {
    let parsed = parse_single_bridge_pool_file(&file.content)
      .context(format!("Failed to parse file: {}", file.path))?;
    parsed_assignments.push(parsed);
  }

  Ok(parsed_assignments)
}

/// Parses a single bridge pool assignment file's content.
///
/// This internal function processes the content of a single file, extracting the timestamp and
/// bridge entries. It expects a "bridge-pool-assignment" line followed by bridge entry lines.
///
/// # Arguments
///
/// * `content` - The string content of the bridge pool assignment file.
///
/// # Returns
///
/// * `Ok(ParsedBridgePoolAssignment)` - The parsed data.
/// * `Err(anyhow::Error)` - An error if parsing fails (e.g., missing or invalid lines).
fn parse_single_bridge_pool_file(content: &str) -> AnyhowResult<ParsedBridgePoolAssignment> {
  let mut lines = content.lines();
  let mut published_millis = None;

  // Find and parse the "bridge-pool-assignment" line
  for line in lines.by_ref() {
    let trimmed = line.trim();
    if trimmed.starts_with("bridge-pool-assignment") {
      published_millis = Some(parse_bridge_pool_assignment_line(trimmed)
        .context("Failed to parse bridge-pool-assignment line")?);
      break;
    }
  }

  // Ensure we found a bridge-pool-assignment line
  let published_millis = published_millis.context("No bridge-pool-assignment line found")?;

  // Parse remaining lines for bridge entries
  let mut entries = BTreeMap::new();
  for line in lines {
    let trimmed = line.trim();
    if let Some((fingerprint, assignment)) = parse_bridge_line(trimmed)? {
      entries.insert(fingerprint, assignment);
    }
  }

  Ok(ParsedBridgePoolAssignment {
    published_millis,
    entries,
  })
}

/// Parses the "bridge-pool-assignment" line to extract the publication timestamp.
///
/// The expected format is "bridge-pool-assignment YYYY-MM-DD HH:MM:SS".
///
/// # Arguments
///
/// * `line` - The line starting with "bridge-pool-assignment" followed by a timestamp.
///
/// # Returns
///
/// * `Ok(i64)` - The timestamp in milliseconds since the epoch.
/// * `Err(anyhow::Error)` - An error if the line is malformed or the timestamp is invalid.
fn parse_bridge_pool_assignment_line(line: &str) -> AnyhowResult<i64> {
  let parts: Vec<&str> = line.split_whitespace().collect();
  if parts.len() != 3 || parts[0] != "bridge-pool-assignment" {
    return Err(anyhow::anyhow!("Invalid bridge-pool-assignment line: {}", line));
  }
  let date = parts[1];
  let time = parts[2];
  let timestamp_str = format!("{} {}", date, time);
  let naive_dt = NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S")
    .context("Failed to parse timestamp")?;
  let published_millis = naive_dt.and_utc().timestamp_millis();
  Ok(published_millis)
}

/// Parses a bridge entry line to extract the fingerprint and assignment string.
///
/// The expected format is "<fingerprint> <assignment>", where <fingerprint> is a 40-character hex string.
///
/// # Arguments
///
/// * `line` - A line containing a fingerprint and assignment details.
///
/// # Returns
///
/// * `Ok(Option<(String, String)>)` - The fingerprint and assignment if valid, `None` if the line is malformed.
/// * `Err(anyhow::Error)` - An error if parsing fails unexpectedly.
fn parse_bridge_line(line: &str) -> AnyhowResult<Option<(String, String)>> {
  let parts: Vec<&str> = line.splitn(2, ' ').collect();
  if parts.len() < 2 {
    return Ok(None); // Skip invalid lines
  }
  let fingerprint = parts[0].to_string();
  let assignment = parts[1].to_string();
  
  Ok(Some((fingerprint, assignment)))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::fetch::BridgePoolFile;

  /// Tests parsing of a valid "bridge-pool-assignment" line.
  #[test]
  fn test_parse_bridge_pool_assignment_line_valid() {
    let line = "bridge-pool-assignment 2022-04-09 00:29:37";
    let published_millis = parse_bridge_pool_assignment_line(line).unwrap();
    assert_eq!(published_millis, 1649464177000);
  }

  /// Tests parsing of an invalid "bridge-pool-assignment" line.
  #[test]
  fn test_parse_bridge_pool_assignment_line_invalid() {
    let line = "bridge-pool-assignment invalid";
    let result = parse_bridge_pool_assignment_line(line);
    assert!(result.is_err());
  }

  /// Tests parsing of a valid bridge entry line.
  #[test]
  fn test_parse_bridge_line_valid() {
    let line = "005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4";
    let (fingerprint, assignment) = parse_bridge_line(line).unwrap().unwrap();
    assert_eq!(fingerprint, "005fd4d7decbb250055b861579e6fdc79ad17bee");
    assert_eq!(assignment, "email transport=obfs4");
  }

  /// Tests parsing of an invalid bridge entry line (too few parts).
  #[test]
  fn test_parse_bridge_line_invalid() {
    let line = "005fd4d7decbb250055b861579e6fdc79ad17bee";
    let result = parse_bridge_line(line).unwrap();
    assert!(result.is_none());
  }

  /// Tests parsing of a single bridge pool assignment file with valid content.
  #[test]
  fn test_parse_single_bridge_pool_file_valid() {
    let content = "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n";
    let parsed = parse_single_bridge_pool_file(content).unwrap();
    assert_eq!(parsed.published_millis, 1649464177000);
    assert_eq!(parsed.entries.len(), 1);
    assert_eq!(parsed.entries["005fd4d7decbb250055b861579e6fdc79ad17bee"], "email transport=obfs4");
  }

  /// Tests parsing of a bridge pool assignment file missing the "bridge-pool-assignment" line.
  #[test]
  fn test_parse_single_bridge_pool_file_missing_header() {
    let content = "005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n";
    let result = parse_single_bridge_pool_file(content);
    assert!(result.is_err());
  }

  /// Tests parsing of multiple bridge pool assignment files with valid content.
  #[test]
  fn test_parse_bridge_pool_files_valid() {
    let files = vec![BridgePoolFile {
      path: "file1".to_string(),
      last_modified: 0,
      content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".to_string(),
    }];
    let parsed = parse_bridge_pool_files(files).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].published_millis, 1649464177000);
    assert_eq!(parsed[0].entries["005fd4d7decbb250055b861579e6fdc79ad17bee"], "email transport=obfs4");
  }

  /// Tests parsing of a bridge pool assignment file with invalid content.
  #[test]
  fn test_parse_bridge_pool_files_invalid() {
    let files = vec![BridgePoolFile {
      path: "file1".to_string(),
      last_modified: 0,
      content: "invalid content".to_string(),
    }];
    let result = parse_bridge_pool_files(files);
    assert!(result.is_err());
  }
}