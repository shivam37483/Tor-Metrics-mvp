//! Tools for exporting parsed bridge pool assignment data to a PostgreSQL database.
//!
//! This module provides functionality to export parsed bridge pool assignment data into a PostgreSQL database.
//! It manages database connections, table creation, and data insertion within a transactional context to ensure
//! consistency. The export process is optimized with batch inserts to handle large datasets efficiently.
//!
//! ## Usage
//!
//! The main entry point is the [`export_to_postgres`] function, which takes a vector of parsed assignments,
//! a database connection string, and a flag to clear existing data. It establishes a connection, sets up tables,
//! and inserts data in a single transaction.
//!
//! ## Dependencies
//!
//! - **`tokio_postgres`**: Asynchronous PostgreSQL client for database operations.
//! - **`chrono`**: Handles timestamp conversions and formatting.
//! - **`sha2`**: Computes SHA-256 digests for data integrity.
//! - **`anyhow`**: Simplifies error handling with context.
//!
//! ## Error Handling
//!
//! All functions return `anyhow::Result` to provide detailed error messages for database failures, parsing issues,
//! or invalid data.

use crate::parse::ParsedBridgePoolAssignment;
use anyhow::{Context, Result as AnyhowResult};
use chrono::{DateTime, Utc};
use tokio_postgres::{NoTls, Transaction};
use sha2::{Digest, Sha256};

// Global constant to limit the number of files to export during testing
const MAX_FILES_TO_EXPORT: usize = 100;

/// Exports parsed bridge pool assignment data to a PostgreSQL database.
///
/// Connects to a PostgreSQL database, creates necessary tables if they don’t exist, and inserts the provided
/// parsed data. Uses a transaction to ensure atomicity across table operations. Optionally truncates existing
/// tables if the `clear` flag is set.
///
/// # Arguments
///
/// * `parsed_assignments` - Vector of parsed bridge pool assignments to export.
/// * `db_params` - PostgreSQL connection string (e.g., "host=localhost user=postgres password=example").
/// * `clear` - If `true`, truncates existing tables before inserting new data.
///
/// # Returns
///
/// * `Ok(())` - Data successfully exported.
/// * `Err(anyhow::Error)` - Connection, transaction, or query execution failed.
///
/// # Examples
///
/// ```rust,no_run
/// use bridge_pool_assignments::parse::ParsedBridgePoolAssignment;
/// use bridge_pool_assignments::export::export_to_postgres;
/// use std::collections::BTreeMap;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     // Create a dummy ParsedBridgePoolAssignment
///     let assignment = ParsedBridgePoolAssignment {
///         published_millis: 1638316800000, // Example timestamp
///         entries: BTreeMap::new(),        // Empty entries for simplicity
///     };
///     let assignments = vec![assignment];
///     export_to_postgres(
///         assignments,
///         "host=localhost user=postgres password=your_password dbname=your_db",
///         false,
///     ).await?;
///     Ok(())
/// }
/// ```
pub async fn export_to_postgres(
  parsed_assignments: Vec<ParsedBridgePoolAssignment>,
  db_params: &str,
  clear: bool,
) -> AnyhowResult<()> {
  let (mut client, connection) = tokio_postgres::connect(db_params, NoTls)
    .await
    .context("Failed to connect to PostgreSQL")?;
  tokio::spawn(async move {
    if let Err(e) = connection.await {
      eprintln!("Database connection error: {}", e);
    }
  });

  let transaction = client
    .transaction()
    .await
    .context("Failed to start transaction")?;

  create_tables(&transaction)
    .await
    .context("Failed to create tables")?;

  if clear {
    transaction
      .execute("TRUNCATE TABLE bridge_pool_assignment CASCADE", &[])
      .await
      .context("Failed to truncate bridge_pool_assignment")?;
    transaction
      .execute("TRUNCATE TABLE bridge_pool_assignments_file CASCADE", &[])
      .await
      .context("Failed to truncate bridge_pool_assignments_file")?;
  }

  let assignments_to_export = parsed_assignments
    .into_iter()
    .take(MAX_FILES_TO_EXPORT)
    .collect::<Vec<_>>();

  for assignment in assignments_to_export {
    let digest = compute_digest(&assignment);
    insert_file_data(&transaction, &assignment, &digest)
      .await
      .context("Failed to insert file data")?;
    insert_assignment_data(&transaction, &assignment, &digest)
      .await
      .context("Failed to insert assignment data")?;
  }

  transaction
    .commit()
    .await
    .context("Failed to commit transaction")?;

  Ok(())
}

/// Creates tables and indexes in the database if they don’t already exist.
///
/// Sets up the schema for `bridge_pool_assignments_file` and `bridge_pool_assignment` tables, including
/// primary keys, foreign key references, and performance-enhancing indexes.
///
/// # Arguments
///
/// * `transaction` - Active database transaction to execute schema creation queries.
///
/// # Returns
///
/// * `Ok(())` - Tables and indexes created successfully.
/// * `Err(anyhow::Error)` - Query execution failed.
async fn create_tables(transaction: &Transaction<'_>) -> AnyhowResult<()> {
  transaction
    .execute(
      "CREATE TABLE IF NOT EXISTS bridge_pool_assignments_file (
        published TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        header TEXT NOT NULL,
        digest TEXT NOT NULL,
        PRIMARY KEY(digest)
      )",
      &[],
    )
    .await
    .context("Failed to create bridge_pool_assignments_file table")?;

  transaction
    .execute(
      "CREATE INDEX IF NOT EXISTS bridge_pool_assignment_file_published 
      ON bridge_pool_assignments_file (published)",
      &[],
    )
    .await
    .context("Failed to create index on bridge_pool_assignments_file")?;

  transaction
    .execute(
      "CREATE TABLE IF NOT EXISTS bridge_pool_assignment (
        id SERIAL PRIMARY KEY,
        published TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        digest TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        distribution_method TEXT NOT NULL,
        transport TEXT,
        ip TEXT,
        blocklist TEXT,
        bridge_pool_assignments TEXT REFERENCES bridge_pool_assignments_file(digest),
        distributed BOOLEAN,
        state TEXT,
        bandwidth TEXT,
        ratio REAL
      )",
      &[],
    )
    .await
    .context("Failed to create bridge_pool_assignment table")?;

  transaction
    .execute(
      "CREATE INDEX IF NOT EXISTS bridge_pool_assignment_published 
      ON bridge_pool_assignment (published)",
      &[],
    )
    .await
    .context("Failed to create published index on bridge_pool_assignment")?;

  transaction
    .execute(
      "CREATE INDEX IF NOT EXISTS bridge_pool_assignment_fingerprint 
      ON bridge_pool_assignment (fingerprint)",
      &[],
    )
    .await
    .context("Failed to create fingerprint index on bridge_pool_assignment")?;

  transaction
    .execute(
      "CREATE INDEX IF NOT EXISTS bridge_pool_assignment_fingerprint_published_desc_index 
      ON bridge_pool_assignment (fingerprint, published DESC)",
      &[],
    )
    .await
    .context("Failed to create composite index on bridge_pool_assignment")?;

  Ok(())
}

/// Computes a SHA-256 digest for a bridge pool assignment.
///
/// Generates a unique hash based on the assignment’s timestamp and entries to ensure data integrity.
///
/// # Arguments
///
/// * `assignment` - The parsed bridge pool assignment to hash.
///
/// # Returns
///
/// A hexadecimal string representing the SHA-256 digest.
fn compute_digest(assignment: &ParsedBridgePoolAssignment) -> String {
  let mut hasher = Sha256::new();
  hasher.update(assignment.published_millis.to_string().as_bytes());
  for (fingerprint, assignment_str) in &assignment.entries {
    hasher.update(fingerprint.as_bytes());
    hasher.update(assignment_str.as_bytes());
  }
  let result = hasher.finalize();
  hex::encode(result)
}

/// Inserts file metadata into the `bridge_pool_assignments_file` table.
///
/// Adds a record for the assignment file, including its publication timestamp, header, and digest.
///
/// # Arguments
///
/// * `transaction` - Active database transaction.
/// * `assignment` - Parsed bridge pool assignment data.
/// * `digest` - SHA-256 digest of the assignment.
///
/// # Returns
///
/// * `Ok(())` - Data inserted successfully.
/// * `Err(anyhow::Error)` - Timestamp conversion or query execution failed.
async fn insert_file_data(
  transaction: &Transaction<'_>,
  assignment: &ParsedBridgePoolAssignment,
  digest: &str,
) -> AnyhowResult<()> {
  let published_dt = DateTime::<Utc>::from_timestamp_millis(assignment.published_millis)
    .context("Invalid published timestamp")?;
  let published_naive = published_dt.naive_utc();

  let header = "bridge-pool-assignment";
  transaction
    .execute(
      "INSERT INTO bridge_pool_assignments_file (published, header, digest) 
      VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING",
      &[&published_naive, &header, &digest],
    )
    .await
    .context("Failed to insert into bridge_pool_assignments_file")?;
  Ok(())
}

/// Inserts individual assignment entries into the `bridge_pool_assignment` table.
///
/// Processes assignment entries in batches for efficiency, parsing each entry into structured fields.
///
/// # Arguments
///
/// * `transaction` - Active database transaction.
/// * `assignment` - Parsed bridge pool assignment data.
/// * `digest` - SHA-256 digest linking to the file table.
///
/// # Returns
///
/// * `Ok(())` - Data inserted successfully.
/// * `Err(anyhow::Error)` - Timestamp conversion or batch insertion failed.
async fn insert_assignment_data(
  transaction: &Transaction<'_>,
  assignment: &ParsedBridgePoolAssignment,
  digest: &str,
) -> AnyhowResult<()> {
  let mut batch_data = Vec::new();
  let batch_size = 1000;

  let published_naive = DateTime::<Utc>::from_timestamp_millis(assignment.published_millis)
    .context("Invalid published timestamp")?
    .naive_utc();

  for (fingerprint, assignment_str) in &assignment.entries {
    let (distribution_method, transport, ip, blocklist, distributed, state, bandwidth, ratio) =
      parse_assignment_string(assignment_str);

    batch_data.push((
      published_naive,
      digest.to_string(),
      fingerprint.to_string(),
      distribution_method,
      transport,
      ip,
      blocklist,
      digest.to_string(),
      distributed.unwrap_or(false),
      state,
      bandwidth,
      ratio,
    ));

    if batch_data.len() >= batch_size {
      insert_batch(&transaction, &batch_data).await?;
      batch_data.clear();
    }
  }

  if !batch_data.is_empty() {
    insert_batch(&transaction, &batch_data).await?;
  }

  Ok(())
}

/// Executes a batch insert into the `bridge_pool_assignment` table.
///
/// Constructs a dynamic SQL query for efficient multi-row insertion.
///
/// # Arguments
///
/// * `transaction` - Active database transaction.
/// * `batch_data` - Vector of tuples containing assignment data.
///
/// # Returns
///
/// * `Ok(())` - Batch inserted successfully.
/// * `Err(anyhow::Error)` - Query execution failed.
async fn insert_batch(
  transaction: &Transaction<'_>,
  batch_data: &[(chrono::NaiveDateTime, String, String, String, Option<String>, Option<String>, Option<String>, String, bool, Option<String>, Option<String>, Option<f32>)],
) -> AnyhowResult<()> {
  let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
  let mut placeholders = Vec::new();

  for (j, data) in batch_data.iter().enumerate() {
    params.extend_from_slice(&[
      &data.0,  // published
      &data.1,  // digest
      &data.2,  // fingerprint
      &data.3,  // distribution_method
      &data.4,  // transport
      &data.5,  // ip
      &data.6,  // blocklist
      &data.7,  // bridge_pool_assignments
      &data.8,  // distributed
      &data.9,  // state
      &data.10, // bandwidth
      &data.11, // ratio
    ]);

    placeholders.push(format!(
      "(${},${},${},${},${},${},${},${},${},${},${},${})",
      j * 12 + 1,  j * 12 + 2,  j * 12 + 3,  j * 12 + 4,
      j * 12 + 5,  j * 12 + 6,  j * 12 + 7,  j * 12 + 8,
      j * 12 + 9,  j * 12 + 10, j * 12 + 11, j * 12 + 12
    ));
  }

  let query = format!(
    "INSERT INTO bridge_pool_assignment (published, digest, fingerprint, distribution_method, transport, ip, blocklist, bridge_pool_assignments, distributed, state, bandwidth, ratio) VALUES {}",
    placeholders.join(",")
  );

  transaction
    .execute(&query, &params)
    .await
    .context("Failed to insert into bridge_pool_assignment")?;

  Ok(())
}

/// Parses an assignment string into structured fields based on BridgeDB conventions.
///
/// Extracts key-value pairs from the assignment string, mapping them to database fields.
///
/// # Arguments
///
/// * `assignment` - Raw assignment string (e.g., "https transport=obfs4 ip=192.168.1.1").
///
/// # Returns
///
/// A tuple containing:
/// - `distribution_method`: Primary distribution method (e.g., "https").
/// - `transport`: Optional transport protocol.
/// - `ip`: Optional IP address.
/// - `blocklist`: Optional blocklist identifier.
/// - `distributed`: Optional boolean indicating distribution status.
/// - `state`: Optional state information.
/// - `bandwidth`: Optional bandwidth value.
/// - `ratio`: Optional ratio as a float.
fn parse_assignment_string(assignment: &str) -> (
  String,
  Option<String>,
  Option<String>,
  Option<String>,
  Option<bool>,
  Option<String>,
  Option<String>,
  Option<f32>,
) {
  let parts: Vec<&str> = assignment.split_whitespace().collect();
  let distribution_method = parts.get(0).unwrap_or(&"unknown").to_string();
  let mut transport = None;
  let mut ip = None;
  let mut blocklist = None;
  let mut distributed = None;
  let mut state = None;
  let mut bandwidth = None;
  let mut ratio = None;

  for part in parts.iter().skip(1) {
    let kv: Vec<&str> = part.split('=').collect();
    if kv.len() == 2 {
      match kv[0] {
        "transport" => transport = Some(kv[1].to_string()),
        "ip" => ip = Some(kv[1].to_string()),
        "blocklist" => blocklist = Some(kv[1].to_string()),
        "distributed" => distributed = kv[1].parse::<bool>().ok(),
        "state" => state = Some(kv[1].to_string()),
        "bandwidth" => bandwidth = Some(kv[1].to_string()),
        "ratio" => ratio = kv[1].parse::<f32>().ok(),
        _ => {}, // Ignore unrecognized keys
      }
    }
  }

  (
    distribution_method,
    transport,
    ip,
    blocklist,
    distributed,
    state,
    bandwidth,
    ratio,
  )
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;

  /// Tests the `compute_digest` function with a sample assignment.
  #[test]
  fn test_compute_digest() {
    let mut assignment = ParsedBridgePoolAssignment {
      published_millis: 1638316800000, // 2021-12-01 00:00:00 UTC
      entries: BTreeMap::new(),
    };
    assignment.entries.insert(
      "fingerprint1".to_string(),
      "https transport=obfs4".to_string(),
    );
    
    let digest = compute_digest(&assignment);
    assert_eq!(
      digest,
      "96b312792d229dbf674e80509373e1d21a45590d4526455345fdd7dd2d6ba4a0"
    );
  }

  /// Tests the `parse_assignment_string` function with various input formats.
  #[test]
  fn test_parse_assignment_string() {
    let input = "https transport=obfs4 ip=192.168.1.1 distributed=true ratio=0.5";
    let (
      distribution_method,
      transport,
      ip,
      blocklist,
      distributed,
      state,
      bandwidth,
      ratio,
    ) = parse_assignment_string(input);
  
    assert_eq!(distribution_method, "https");
    assert_eq!(transport, Some("obfs4".to_string()));
    assert_eq!(ip, Some("192.168.1.1".to_string()));
    assert_eq!(blocklist, None);
    assert_eq!(distributed, Some(true));
    assert_eq!(state, None);
    assert_eq!(bandwidth, None);
    assert_eq!(ratio, Some(0.5));
  
    // Test minimal input
    let input = "email";
    let (
      distribution_method,
      transport,
      ip,
      blocklist,
      distributed,
      state,
      bandwidth,
      ratio,
    ) = parse_assignment_string(input);
  
    assert_eq!(distribution_method, "email");
    assert_eq!(transport, None);
    assert_eq!(ip, None);
    assert_eq!(blocklist, None);
    assert_eq!(distributed, None);
    assert_eq!(state, None);
    assert_eq!(bandwidth, None);
    assert_eq!(ratio, None);
  }
}