//! Tools for exporting parsed metrics to a PostgreSQL database.

use std::collections::HashMap;
use std::error::Error;
use tokio_postgres::NoTls;

/// Exports parsed metrics to a PostgreSQL database.
///
/// This function establishes a connection to a PostgreSQL database, creates the `tor_metrics`
/// table if it doesn’t exist, and inserts each metric from the provided map. Metrics are stored
/// with their name, value, and an automatic timestamp.
///
/// # Arguments
/// - `metrics`: A map of metric names to their values (e.g., "relay_count" -> 1000).
/// - `db_params`: PostgreSQL connection string (e.g., "host=localhost user=postgres").
///
/// # Returns
/// - `Ok(())` if all metrics are successfully exported.
/// - `Err(Box<dyn Error>)` if the connection fails or a query cannot be executed.
///
/// # Examples
/// ```rust
/// use std::collections::HashMap;
/// use tor_metrics_mvp::export::export_to_postgres;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   let metrics = HashMap::from([("relay_count".to_string(), 1000)]);
///   let db_params = "host=localhost user=postgres password=example";
///   export_to_postgres(metrics, db_params).await?;
///   Ok(())
/// }
/// ```
///
/// # Notes
/// - Assumes `metrics` contains `u64` values, which are cast to `i64` for PostgreSQL’s `BIGINT`.
/// - The `tor_metrics` table has columns:
///   - `id`: Auto-incrementing primary key.
///   - `metric_name`: Text name of the metric.
///   - `metric_value`: Numeric value of the metric (stored as `BIGINT`).
///   - `timestamp`: Insertion time, set to the current time by default.
/// - Connection errors are printed to stderr; consider logging in production.
pub async fn export_to_postgres(metrics: HashMap<String, u64>, db_params: &str, clear: bool) -> Result<(), Box<dyn Error>> {
  // Connect to PostgreSQL
  let (client, connection) = tokio_postgres::connect(db_params, NoTls).await?;
  tokio::spawn(async move {
    if let Err(e) = connection.await {
        eprintln!("Database connection error: {}", e);
    }
  });

  if clear {
    client.execute("TRUNCATE TABLE tor_metrics RESTART IDENTITY", &[]).await?;
  }

  // Create table if it doesn’t exist
  client
    .execute(
      "CREATE TABLE IF NOT EXISTS tor_metrics (
      id SERIAL PRIMARY KEY,
      metric_name TEXT NOT NULL,
      metric_value BIGINT NOT NULL,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      )",
      &[],
    )
    .await?;

    // Insert metrics
  for (key, value) in metrics {
    client
      .execute(
        "INSERT INTO tor_metrics (metric_name, metric_value) VALUES ($1, $2)",
        &[&key, &(value as i64)], // Cast u64 to i64 for BIGINT
      )
      .await?;
    }

  Ok(())
}