use super::types::BridgePoolFile;
use anyhow::{Context, Result as AnyhowResult};
use chrono::NaiveDateTime;
use futures::future::join_all;
use log::{error, info};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

/// Fetches bridge pool assignment files from a CollecTor instance.
///
/// This function orchestrates the fetching process by retrieving the `index.json`, filtering files
/// from the specified directories based on a minimum last-modified timestamp, and fetching their
/// contents concurrently. The function limits the number of files fetched to MAX_FILES_TO_FETCH (100)
/// to prevent excessive resource consumption.
///
/// # Arguments
///
/// * `collec_tor_base_url` - Base URL of the CollecTor instance (e.g., "https://collector.torproject.org").
/// * `dirs` - List of directories to fetch files from (e.g., ["recent/bridge-pool-assignments"]).
/// * `min_last_modified` - Minimum last-modified timestamp in milliseconds (use 0 to include all files).
///
/// # Returns
///
/// * `Ok(Vec<BridgePoolFile>)` - A vector of fetched bridge pool files.
/// * `Err(anyhow::Error)` - An error if fetching or processing fails.
///
/// # Examples
///
/// ```rust
/// use bridge_pool_assignments::fetch::fetch_bridge_pool_files;
/// use anyhow::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let files = fetch_bridge_pool_files(
///     "https://collector.torproject.org",
///     &["recent/bridge-pool-assignments"],
///     0,
///   ).await?;
///   println!("Fetched {} files", files.len());
///   Ok(())
/// }
/// ```
pub async fn fetch_bridge_pool_files(
    collec_tor_base_url: &str,
    dirs: &[&str],
    min_last_modified: i64,
) -> AnyhowResult<Vec<BridgePoolFile>> {
    let base_url = normalize_url(collec_tor_base_url);
    let index = fetch_index(&base_url).await.context("Failed to fetch index.json")?;
    let remote_files = collect_remote_files(&index, dirs, min_last_modified)
        .context("Failed to collect remote files")?;
    let bridge_files = fetch_file_contents(&base_url, remote_files)
        .await
        .context("Failed to fetch file contents")?;
    info!("Completed fetching {} files", bridge_files.len());
    Ok(bridge_files)
}

/// Normalizes the base URL by ensuring it ends with a trailing slash.
///
/// This helper function ensures consistent URL formatting for subsequent HTTP requests.
///
/// # Arguments
///
/// * `url` - The base URL to normalize.
///
/// # Returns
///
/// A `String` representing the normalized URL with a trailing slash.
fn normalize_url(url: &str) -> String {
    if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{}/", url)
    }
}

/// Fetches and parses the `index.json` from a CollecTor instance.
///
/// # Arguments
///
/// * `base_url` - The normalized base URL of the CollecTor instance.
///
/// # Returns
///
/// * `Ok(Value)` - The parsed JSON value of the index.
/// * `Err(anyhow::Error)` - An error if fetching or parsing fails.
async fn fetch_index(base_url: &str) -> AnyhowResult<Value> {
    let index_url = format!("{}index/index.json", base_url);
    let resp = reqwest::get(&index_url)
        .await
        .context("Failed to get index.json")?;
    let index: Value = resp.json().await.context("Failed to parse index.json")?;
    Ok(index)
}

/// Collects file paths and timestamps from the index for specified directories.
///
/// This function filters files based on the minimum last-modified timestamp and aggregates them
/// from the provided directories.
///
/// # Arguments
///
/// * `index` - The parsed JSON index from CollecTor.
/// * `remote_directories` - List of directories to collect files from.
/// * `min_last_modified` - Minimum last-modified timestamp in milliseconds.
///
/// # Returns
///
/// * `Ok(Vec<(String, i64)>)` - A vector of (file path, last modified timestamp) pairs.
/// * `Err(anyhow::Error)` - An error if no files are found or parsing fails.
fn collect_remote_files(
    index: &Value,
    remote_directories: &[&str],
    min_last_modified: i64,
) -> AnyhowResult<Vec<(String, i64)>> {
    let mut all_files = Vec::new();
    for dir in remote_directories {
        let files = collect_files_from_dir(index, dir, min_last_modified)
            .context(format!("Failed to collect files from directory: {}", dir))?;
        all_files.extend(files);
    }
    if all_files.is_empty() {
        return Err(anyhow::anyhow!(
            "No bridge pool assignment files found in directories: {:?}",
            remote_directories
        ));
    }
    Ok(all_files)
}

/// Collects files from a single directory within the index.
///
/// This function traverses the directory structure in the index and collects files that meet the
/// timestamp criteria.
///
/// # Arguments
///
/// * `index` - The parsed JSON index from CollecTor.
/// * `dir` - The directory path to collect files from.
/// * `min_last_modified` - Minimum last-modified timestamp in milliseconds.
///
/// # Returns
///
/// * `Ok(Vec<(String, i64)>)` - A vector of (file path, last modified timestamp) pairs.
/// * `Err(anyhow::Error)` - An error if the directory is not found or parsing fails.
fn collect_files_from_dir(
    index: &Value,
    dir: &str,
    min_last_modified: i64,
) -> AnyhowResult<Vec<(String, i64)>> {
    // Limit the number of files to fetch (same as export limit)
    const MAX_FILES_TO_FETCH: usize = 100;
    
    let mut all_files = Vec::new();
    let dir_path: Vec<&str> = dir.trim_matches('/').split('/').collect();
    let mut current = &index["directories"];
    let mut full_path = String::new();

    info!("Starting traversal for directory: {}", dir);
    for (i, &part) in dir_path.iter().enumerate() {
        if let Some(dirs) = current.as_array() {
            if let Some(next) = dirs.iter().find(|d| d["path"] == part) {
                if !full_path.is_empty() {
                    full_path.push('/');
                }
                full_path.push_str(part);
                info!("Found directory: {} at full path: {}", part, full_path);

                if i == dir_path.len() - 1 {
                    if let Some(files) = next["files"].as_array() {
                        info!("Found {} files in {}", files.len(), full_path);
                        
                        // Sort files by last_modified (newest first) before limiting
                        let mut sorted_files = Vec::new();
                        for file in files {
                            let file_path = file["path"]
                                .as_str()
                                .context("Missing file path")?
                                .to_string();
                            let last_modified_str = file["last_modified"]
                                .as_str()
                                .context("Missing last modified")?;
                            let last_modified = NaiveDateTime::parse_from_str(
                                last_modified_str,
                                "%Y-%m-%d %H:%M",
                            ).map_err(|e| anyhow::anyhow!("Invalid timestamp {}: {}", last_modified_str, e))?;
                            
                            let last_modified_ms = last_modified.and_utc().timestamp_millis();

                            if last_modified_ms >= min_last_modified {
                                sorted_files.push((file_path, last_modified_ms));
                            }
                        }
                        
                        // Sort by newest first
                        sorted_files.sort_by(|a, b| b.1.cmp(&a.1));
                        
                        // Take only MAX_FILES_TO_FETCH newest files
                        for (file_path, last_modified_ms) in sorted_files.into_iter().take(MAX_FILES_TO_FETCH) {
                            let full_file_path = format!("{}/{}", full_path, file_path);
                            all_files.push((full_file_path, last_modified_ms));
                        }
                    }
                } else {
                    current = &next["directories"];
                }
            } else {
                return Err(anyhow::anyhow!("Directory not found: {} in {}", part, full_path));
            }
        } else {
            return Err(anyhow::anyhow!("Expected array of directories"));
        }
    }

    Ok(all_files)
}

/// Fetches the contents of multiple files concurrently.
///
/// This function uses tokio's async runtime and a semaphore to limit concurrent requests,
/// preventing server overload.
///
/// # Arguments
///
/// * `base_url` - The normalized base URL of the CollecTor instance.
/// * `remote_files` - A vector of (file path, last modified timestamp) pairs.
///
/// # Returns
///
/// * `Ok(Vec<BridgePoolFile>)` - A vector of fetched file contents.
/// * `Err(anyhow::Error)` - An error if fetching fails for any file.
async fn fetch_file_contents(
    base_url: &str,
    remote_files: Vec<(String, i64)>,
) -> AnyhowResult<Vec<BridgePoolFile>> {
    // Limit to 50 concurrent requests to avoid overwhelming the server
    let semaphore = Arc::new(Semaphore::new(50));
    
    // Create a task for each file to fetch
    let fetch_tasks: Vec<JoinHandle<AnyhowResult<BridgePoolFile>>> = remote_files
        .into_iter()
        .map(|(path, _)| {
            let base_url = base_url.to_string();
            let path = path.to_string();
            let semaphore = Arc::clone(&semaphore);
            
            let permit = semaphore.acquire_owned();
            tokio::spawn(async move {
                let _permit = permit.await.context("Failed to acquire semaphore")?;
                let content = fetch_file_content(&base_url, &path)
                    .await
                    .context(format!("Failed to fetch content for {}", path))?;
                info!("Fetched content for {}", path);
                
                Ok(content)
            })
        })
        .collect();

    let results = join_all(fetch_tasks).await;
    let mut bridge_files = Vec::new();
    let mut errors = 0;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(file)) => bridge_files.push(file),
            Ok(Err(e)) => {
                error!("Task {} failed: {:?}", i, e);
                errors += 1;
            }
            Err(e) => {
                error!("Task {} panicked: {:?}", i, e);
                errors += 1;
            }
        }
    }

    info!(
        "Fetched {} files successfully, {} errors encountered",
        bridge_files.len(),
        errors
    );
    Ok(bridge_files)
}

/// Fetches the content of a single file from CollecTor.
///
/// Retrieves both the text content and raw bytes of the file for both parsing and
/// digest calculation. The last-modified timestamp is extracted from the response headers.
///
/// # Arguments
///
/// * `base_url` - The normalized base URL of the CollecTor instance.
/// * `file_path` - The relative path of the file to fetch.
///
/// # Returns
///
/// * `Ok(BridgePoolFile)` - The fetched file with content, raw bytes, and metadata.
/// * `Err(anyhow::Error)` - An error if fetching or reading the file fails.
async fn fetch_file_content(base_url: &str, file_path: &str) -> AnyhowResult<BridgePoolFile> {
    let file_url = format!("{}{}", base_url, file_path);
    let resp = reqwest::get(&file_url)
        .await
        .context("Failed to get file")?;
        
    // Extract last_modified from headers
    let last_modified = if let Some(last_mod_header) = resp.headers().get("Last-Modified") {
        if let Ok(last_mod_str) = last_mod_header.to_str() {
            // Parse date header to timestamp
            chrono::DateTime::parse_from_rfc2822(last_mod_str)
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };
    
    // Get the text content first (this consumes the response)
    let text = resp.text().await.context("Failed to get response text")?;
    
    // Use the text content to also create raw_content
    let raw_content = text.as_bytes().to_vec();
    
    Ok(BridgePoolFile {
        path: file_path.to_string(),
        last_modified,
        content: text,
        raw_content,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the `normalize_url` function to ensure it correctly adds a trailing slash.
    #[test]
    fn test_normalize_url() {
        assert_eq!(
            normalize_url("https://example.com"),
            "https://example.com/"
        );
        assert_eq!(
            normalize_url("https://example.com/"),
            "https://example.com/"
        );
    }
} 