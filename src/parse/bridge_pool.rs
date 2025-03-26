use super::types::ParsedBridgePoolAssignment;
use crate::fetch::BridgePoolFile;
use anyhow::{Context, Result as AnyhowResult};
use chrono::NaiveDateTime;
use std::collections::BTreeMap;

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
/// use bridge_pool_assignments::fetch::BridgePoolFile;
/// use bridge_pool_assignments::parse::parse_bridge_pool_files;
/// let files = vec![BridgePoolFile {
///   path: "file1".to_string(),
///   last_modified: 0,
///   content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".to_string(),
///   raw_content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".as_bytes().to_vec(),
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
        let parsed = parse_single_bridge_pool_file(&file.content, file.raw_content)
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
/// * `raw_content` - The raw bytes of the file content for digest calculation.
///
/// # Returns
///
/// * `Ok(ParsedBridgePoolAssignment)` - The parsed data.
/// * `Err(anyhow::Error)` - An error if parsing fails (e.g., missing or invalid lines).
fn parse_single_bridge_pool_file(content: &str, raw_content: Vec<u8>) -> AnyhowResult<ParsedBridgePoolAssignment> {
    let mut lines = content.lines();
    let mut published_millis = None;
    let mut raw_lines = BTreeMap::new();

    // Find and parse the "bridge-pool-assignment" line
    let mut header_line = None;
    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.starts_with("bridge-pool-assignment") {
            published_millis = Some(parse_bridge_pool_assignment_line(trimmed)
                .context("Failed to parse bridge-pool-assignment line")?);
            header_line = Some(trimmed);
            break;
        }
    }

    // Ensure we found a bridge-pool-assignment line
    let published_millis = published_millis.context("No bridge-pool-assignment line found")?;

    // Parse remaining lines for bridge entries
    let mut entries = BTreeMap::new();
    
    // Reset lines iterator to process from beginning for raw line capture
    let content_lines = content.lines();
    
    for line in content_lines {
        let trimmed = line.trim();
        
        // Skip header line, we already processed it
        if Some(trimmed) == header_line {
            continue;
        }
        
        if let Some((fingerprint, assignment)) = parse_bridge_line(trimmed)? {
            entries.insert(fingerprint.clone(), assignment);
            // Store raw line bytes for digest calculation
            raw_lines.insert(fingerprint, trimmed.as_bytes().to_vec());
        }
    }

    Ok(ParsedBridgePoolAssignment {
        published_millis,
        entries,
        raw_content,
        raw_lines,
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

    /// Tests parsing a valid bridge pool assignment file.
    #[test]
    fn test_parse_single_bridge_pool_file_valid() {
        let content = "\
bridge-pool-assignment 2022-04-09 00:29:37
005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4
01ea4fb2da2086e71e7ca84c683fcadd2aa9036b email transport=obfs4
";
        let raw_content = content.as_bytes().to_vec();
        let result = parse_single_bridge_pool_file(content, raw_content).unwrap();
        
        assert_eq!(result.published_millis, 1649464177000);
        assert_eq!(result.entries.len(), 2);
        assert_eq!(
            result.entries["005fd4d7decbb250055b861579e6fdc79ad17bee"],
            "email transport=obfs4"
        );
        assert_eq!(
            result.entries["01ea4fb2da2086e71e7ca84c683fcadd2aa9036b"],
            "email transport=obfs4"
        );
        assert!(result.raw_lines.contains_key("005fd4d7decbb250055b861579e6fdc79ad17bee"));
        assert!(result.raw_lines.contains_key("01ea4fb2da2086e71e7ca84c683fcadd2aa9036b"));
    }

    /// Tests parsing a bridge pool assignment file with an invalid header.
    #[test]
    fn test_parse_single_bridge_pool_file_invalid_header() {
        let content = "\
invalid-header 2022-04-09 00:29:37
005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4
";
        let raw_content = content.as_bytes().to_vec();
        let result = parse_single_bridge_pool_file(content, raw_content);
        
        assert!(result.is_err());
    }

    /// Tests parsing a bridge pool assignment file with an invalid timestamp format.
    #[test]
    fn test_parse_bridge_pool_assignment_line_invalid_timestamp() {
        let line = "bridge-pool-assignment 2022-04-09 00:29"; // Missing seconds
        let result = parse_bridge_pool_assignment_line(line);
        
        assert!(result.is_err());
    }

    /// Tests parsing multiple bridge pool assignment files.
    #[test]
    fn test_parse_bridge_pool_files() {
        let files = vec![
            BridgePoolFile {
                path: "file1".to_string(),
                last_modified: 0,
                content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".to_string(),
                raw_content: "bridge-pool-assignment 2022-04-09 00:29:37\n005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4\n".as_bytes().to_vec(),
            },
            BridgePoolFile {
                path: "file2".to_string(),
                last_modified: 0,
                content: "bridge-pool-assignment 2022-04-10 00:29:37\n01ea4fb2da2086e71e7ca84c683fcadd2aa9036b email transport=obfs4\n".to_string(),
                raw_content: "bridge-pool-assignment 2022-04-10 00:29:37\n01ea4fb2da2086e71e7ca84c683fcadd2aa9036b email transport=obfs4\n".as_bytes().to_vec(),
            },
        ];
        
        let parsed = parse_bridge_pool_files(files).unwrap();
        
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].published_millis, 1649464177000);
        assert_eq!(parsed[1].published_millis, 1649550577000);
        assert_eq!(parsed[0].entries.len(), 1);
        assert_eq!(parsed[1].entries.len(), 1);
    }
} 