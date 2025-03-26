use crate::parse::ParsedBridgePoolAssignment;
use crate::utils::{compute_file_digest, compute_assignment_digest};
use anyhow::{Context, Result as AnyhowResult};
use chrono::{DateTime, Utc};
use tokio_postgres::{NoTls, Transaction};

// Global constant to limit the number of files to export during testing
const MAX_FILES_TO_EXPORT: usize = 100;

/// Exports parsed bridge pool assignment data to a PostgreSQL database.
///
/// Connects to a PostgreSQL database, creates necessary tables if they don't exist, and inserts the provided
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
///         raw_content: Vec::new(),         // Empty raw content for simplicity
///         raw_lines: BTreeMap::new(),      // Empty raw lines for simplicity
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
    // Use raw content to compute the file digest
    let file_digest = compute_file_digest(&assignment.raw_content);
    
    insert_file_data(&transaction, &assignment, &file_digest)
      .await
      .context("Failed to insert file data")?;
    
    insert_assignment_data(&transaction, &assignment, &file_digest)
      .await
      .context("Failed to insert assignment data")?;
  }

  transaction
    .commit()
    .await
    .context("Failed to commit transaction")?;

  Ok(())
}

/// Creates tables and indexes in the database if they don't already exist.
///
/// Sets up the schema for `bridge_pool_assignments_file` and `bridge_pool_assignment` tables, including
/// primary keys, foreign key references, and performance-enhancing indexes.
///
/// The schema follows the maintainer's recommendations:
/// - `bridge_pool_assignments_file` uses the SHA-256 digest of the raw file content as its primary key
/// - `bridge_pool_assignment` uses the SHA-256 digest of the raw line bytes combined with the file digest as its primary key
/// - A foreign key relationship connects the two tables through the file digest
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
        ratio REAL,
        PRIMARY KEY(digest)
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
    .context("Failed to create fingerprint+published index on bridge_pool_assignment")?;

  Ok(())
}

/// Inserts file metadata into the `bridge_pool_assignments_file` table.
///
/// Adds a record for the assignment file, including its publication timestamp, header, and digest.
///
/// # Arguments
///
/// * `transaction` - Active database transaction.
/// * `assignment` - Parsed bridge pool assignment data.
/// * `digest` - SHA-256 digest of the assignment file's raw content.
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
/// Each entry has its own unique digest calculated from the raw line bytes combined with the file digest.
///
/// # Arguments
///
/// * `transaction` - Active database transaction.
/// * `assignment` - Parsed bridge pool assignment data.
/// * `file_digest` - SHA-256 digest linking to the file table.
///
/// # Returns
///
/// * `Ok(())` - Data inserted successfully.
/// * `Err(anyhow::Error)` - Timestamp conversion or batch insertion failed.
async fn insert_assignment_data(
  transaction: &Transaction<'_>,
  assignment: &ParsedBridgePoolAssignment,
  file_digest: &str,
) -> AnyhowResult<()> {
  let mut batch_data = Vec::new();
  let batch_size = 1000;

  let published_naive = DateTime::<Utc>::from_timestamp_millis(assignment.published_millis)
    .context("Invalid published timestamp")?
    .naive_utc();

  for (fingerprint, assignment_str) in &assignment.entries {
    // Get the raw line bytes for this assignment
    let raw_line = assignment.raw_lines.get(fingerprint)
      .context(format!("No raw line data found for fingerprint: {}", fingerprint))?;
    
    // Compute a unique digest for this assignment
    let digest = compute_assignment_digest(raw_line, file_digest);
    
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
      file_digest.to_string(), // Use file_digest as the foreign key
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
    let base = j * 12;
    let placeholder = format!("(${},${},${},${},${},${},${},${},${},${},${},${})",
      base + 1, base + 2, base + 3, base + 4, base + 5, base + 6,
      base + 7, base + 8, base + 9, base + 10, base + 11, base + 12);
    placeholders.push(placeholder);
  }

  let sql = format!(
    "INSERT INTO bridge_pool_assignment (
      published, digest, fingerprint, distribution_method, transport, ip, 
      blocklist, bridge_pool_assignments, distributed, state, bandwidth, ratio
    ) VALUES {} ON CONFLICT (digest) DO NOTHING",
    placeholders.join(",")
  );

  transaction
    .execute(sql.as_str(), &params)
    .await
    .context("Failed to insert batch into bridge_pool_assignment")?;
  
  Ok(())
}

/// Parses an assignment string into structured fields.
///
/// Extracts various assignment properties from the string representation.
///
/// # Arguments
///
/// * `assignment_str` - The assignment string (e.g., "email transport=obfs4").
///
/// # Returns
///
/// A tuple of extracted fields in the format:
/// (distribution_method, transport, ip, blocklist, distributed, state, bandwidth, ratio)
fn parse_assignment_string(assignment_str: &str) -> (
  String, 
  Option<String>,
  Option<String>,
  Option<String>,
  Option<bool>,
  Option<String>,
  Option<String>,
  Option<f32>
) {
  // Extract distribution method (first token)
  let parts: Vec<&str> = assignment_str.splitn(2, ' ').collect();
  let distribution_method = parts[0].to_string();
  
  // Default return values
  let mut transport = None;
  let mut ip = None;
  let mut blocklist = None;
  let mut distributed = None;
  let mut state = None;
  let mut bandwidth = None;
  let mut ratio = None;
  
  if parts.len() > 1 {
    // Process key=value pairs
    let rest = parts[1];
    let pairs: Vec<&str> = rest.split_whitespace().collect();
    
    for pair in pairs {
      let kv: Vec<&str> = pair.splitn(2, '=').collect();
      if kv.len() == 2 {
        match kv[0] {
          "transport" => transport = Some(kv[1].to_string()),
          "ip" => ip = Some(kv[1].to_string()),
          "blocklist" => blocklist = Some(kv[1].to_string()),
          "distributed" => distributed = Some(kv[1].to_lowercase() == "true"),
          "state" => state = Some(kv[1].to_string()),
          "bandwidth" => bandwidth = Some(kv[1].to_string()),
          "ratio" => ratio = kv[1].parse::<f32>().ok(),
          _ => {} // Ignore unknown properties
        }
      }
    }
  }
  
  (distribution_method, transport, ip, blocklist, distributed, state, bandwidth, ratio)
} 