//! Fetching Tor consensus documents from CollecTor for PostgreSQL export.
//!
//! This module provides functionality to fetch consensus documents from a CollecTor
//! instance, such as "https://collector.torproject.org", and prepares them as structured
//! data suitable for insertion into a PostgreSQL database. It retrieves the `index.json`,
//! filters files based on specified directories and a minimum last-modified timestamp,
//! and collects file contents and metadata.

use chrono::NaiveDateTime;
use serde_json::Value;
use std::error::Error;

/// Represents a fetched file's metadata and content for PostgreSQL insertion.
///
/// This struct holds the essential information about a consensus file fetched from
/// CollecTor, making it ready for database export.
#[derive(Debug)]
pub struct ConsensusFile {
  /// The relative path of the file (e.g., "recent/relay-descriptors/consensuses/2023-10-01-00-00-00-consensus").
  pub path: String,
  /// The last modified timestamp in milliseconds since the Unix epoch.
  pub last_modified: i64,
  /// The textual content of the file.
  pub content: String,
}

/// Fetches consensus documents from CollecTor and prepares them for PostgreSQL.
///
/// This is the main entry point for fetching files. It retrieves the `index.json` from
/// the CollecTor instance, filters files in the specified directories by the minimum
/// last-modified timestamp, and fetches their contents, returning a vector of
/// `ConsensusFile` structs ready for PostgreSQL insertion.
///
/// # Arguments
/// * `collec_tor_base_url` - The base URL of the CollecTor instance (e.g., "https://collector.torproject.org").
/// * `remote_directories` - A slice of directory paths to fetch from (e.g., ["recent/relay-descriptors/consensuses"]).
/// * `min_last_modified` - The minimum last-modified timestamp in milliseconds since the Unix epoch (use 0 for all files).
///
/// # Returns
/// * `Ok(Vec<ConsensusFile>)` - A vector of `ConsensusFile` structs containing file metadata and contents.
/// * `Err(Box<dyn Error>)` - An error if fetching, parsing, or processing fails critically.
///
/// # Examples
/// ```rust
/// use fetch::fetch_consensus_files;
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   let files = fetch_consensus_files(
///     "https://collector.torproject.org",
///     &["recent/relay-descriptors/consensuses"],
///     0,
///   ).await?;
///   for file in files {
///     println!("Fetched: {} (modified: {})", file.path, file.last_modified);
///   }
///   Ok(())
/// }
/// ```
pub async fn fetch_consensus_files(
  collec_tor_base_url: &str,
  remote_directories: &[&str],
  min_last_modified: i64,
) -> Result<Vec<ConsensusFile>, Box<dyn Error>> {
  let base_url = normalize_url(collec_tor_base_url);
  let index = fetch_index(&base_url).await?;
  let remote_files = collect_remote_files(&index, remote_directories, min_last_modified)?;
  let consensus_files = fetch_file_contents(&base_url, remote_files).await?;
  Ok(consensus_files)
}

/// Normalizes the base URL by ensuring it ends with a trailing slash.
///
/// # Arguments
/// * `url` - The input URL to normalize.
///
/// # Returns
/// A `String` representing the normalized URL.
///
/// # Examples
/// ```rust
/// assert_eq!(normalize_url("https://example.com"), "https://example.com/");
/// assert_eq!(normalize_url("https://example.com/"), "https://example.com/");
/// ```
fn normalize_url(url: &str) -> String {
  if url.ends_with('/') {
    url.to_string()
  } else {
    format!("{}/", url)
  }
}

/// Fetches and parses the `index.json` from the CollecTor instance.
///
/// # Arguments
/// * `base_url` - The normalized base URL of the CollecTor instance.
///
/// # Returns
/// * `Ok(Value)` - The parsed JSON value of `index.json`.
/// * `Err(Box<dyn Error>)` - An error if the request or JSON parsing fails.
async fn fetch_index(base_url: &str) -> Result<Value, Box<dyn Error>> {
  let index_url = format!("{}index/index.json", base_url);
  let resp = reqwest::get(&index_url).await?;
  let index: Value = resp.json().await?;
  Ok(index)
}

/// Collects remote file paths and their last-modified timestamps from the index.
///
/// Traverses the `index.json` to find files in the specified directories that meet
/// the minimum last-modified timestamp requirement.
///
/// # Arguments
/// * `index` - The parsed `index.json` as a `serde_json::Value`.
/// * `remote_directories` - A slice of directory paths to search.
/// * `min_last_modified` - The minimum last-modified timestamp in milliseconds.
///
/// # Returns
/// * `Ok(Vec<(String, i64)>)` - A vector of tuples containing file paths and their last-modified timestamps.
/// * `Err(Box<dyn Error>)` - An error if no files are found or parsing fails.
fn collect_remote_files(
  index: &Value,
  remote_directories: &[&str],
  min_last_modified: i64,
) -> Result<Vec<(String, i64)>, Box<dyn Error>> {
  let mut all_files = Vec::new();
  for dir in remote_directories {
    let files = collect_files_from_dir(index, dir, min_last_modified)?;
    all_files.extend(files);
  }
  if all_files.is_empty() {
    return Err(format!("No files found in directories: {:?}", remote_directories).into());
  }
  Ok(all_files)
}

/// Collects files from a single directory within the index.
///
/// # Arguments
/// * `index` - The parsed `index.json` as a `serde_json::Value`.
/// * `dir` - The directory path to search (e.g., "recent/relay-descriptors/consensuses").
/// * `min_last_modified` - The minimum last-modified timestamp in milliseconds.
///
/// # Returns
/// * `Ok(Vec<(String, i64)>)` - A vector of tuples containing file paths and timestamps.
/// * `Err(Box<dyn Error>)` - An error if the directory structure is invalid or timestamps cannot be parsed.
fn collect_files_from_dir(
  index: &Value,
  dir: &str,
  min_last_modified: i64,
) -> Result<Vec<(String, i64)>, Box<dyn Error>> {
  let mut all_files = Vec::new();
  let dir_path: Vec<&str> = dir.trim_matches('/').split('/').collect();
  let mut current = &index["directories"];
  let mut full_path = String::new();

  for (i, &part) in dir_path.iter().enumerate() {
    if let Some(dirs) = current.as_array() {
      if let Some(next) = dirs.iter().find(|d| d["path"] == part) {
        if !full_path.is_empty() {
          full_path.push('/');
        }
        full_path.push_str(part);

        if i == dir_path.len() - 1 {
          if let Some(files) = next["files"].as_array() {
            for file in files {
              let file_path = file["path"].as_str().ok_or("Missing file path")?.to_string();
              let last_modified_str = file["last_modified"].as_str().ok_or("Missing last modified")?;
              let last_modified = NaiveDateTime::parse_from_str(last_modified_str, "%Y-%m-%d %H:%M")
                .map_err(|e| format!("Invalid timestamp {}: {}", last_modified_str, e))?;
              let last_modified_ms = last_modified.and_utc().timestamp_millis();

              if last_modified_ms >= min_last_modified {
                let full_file_path = format!("{}/{}", full_path, file_path);
                all_files.push((full_file_path, last_modified_ms));
              }
            }
          }
          } else {
            current = next.get("directories").ok_or_else(|| format!("No directories under {}", part))?;
          }
          } else {
            break;
          }
      } else {
        break;
      }
  }
  Ok(all_files)
}

/// Fetches the contents of the specified files from CollecTor.
///
/// # Arguments
/// * `base_url` - The normalized base URL of the CollecTor instance.
/// * `remote_files` - A vector of tuples containing file paths and timestamps.
///
/// # Returns
/// * `Ok(Vec<ConsensusFile>)` - A vector of `ConsensusFile` structs with file contents.
/// * `Err(Box<dyn Error>)` - An error if any file fetch fails.
async fn fetch_file_contents(
  base_url: &str,
  remote_files: Vec<(String, i64)>,
) -> Result<Vec<ConsensusFile>, Box<dyn Error>> {
  let mut consensus_files = Vec::new();
  for (path, last_modified) in remote_files {
    let content = fetch_file_content(base_url, &path).await?;
    consensus_files.push(ConsensusFile {
      path,
      last_modified,
      content,
    });
  }
  Ok(consensus_files)
}

/// Fetches the content of a single file from the CollecTor instance.
///
/// # Arguments
/// * `base_url` - The normalized base URL of the CollecTor instance.
/// * `file_path` - The relative path of the file to fetch.
///
/// # Returns
/// * `Ok(String)` - The textual content of the file.
/// * `Err(Box<dyn Error>)` - An error if the request or text extraction fails.
async fn fetch_file_content(base_url: &str, file_path: &str) -> Result<String, Box<dyn Error>> {
  let file_url = format!("{}{}", base_url, file_path);
  let resp = reqwest::get(&file_url).await?;
  let content = resp.text().await?;
  Ok(content)
}