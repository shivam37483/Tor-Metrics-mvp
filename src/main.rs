//! Tor Metrics MVP: Fetch, Parse, and Export Consensus Documents to PostgreSQL
//!
//! This application demonstrates fetching Tor network consensus documents from CollecTor,
//! parsing their contents into metrics, and exporting the results to a PostgreSQL database.
//! It mirrors the style and structure of the Tor Project's `metrics-lib`, adapted for Rust.
//!
//! ## Purpose
//! The goal is to showcase a minimal yet functional pipeline for processing Tor consensus
//! data in Rust, with the final step being storage in a PostgreSQL database.
//!
//! ## Design Overview
//! - **Fetching**: Retrieves consensus documents from CollecTor using the `fetch` module.
//! - **Parsing**: Extracts metrics (e.g., relay counts) using the `parse` module.
//! - **Exporting**: Saves parsed metrics to a PostgreSQL database via the `export` module.
//!
//! ## Dependencies
//! - **`reqwest`**: For HTTP requests to fetch data from CollecTor.
//! - **`tokio`**: For asynchronous runtime to handle network and database operations.
//! - **`tokio-postgres`**: For PostgreSQL database interaction.
//! - **`log` and `env_logger`**: For structured logging instead of `println!`.
//! - **`clap`**: For parsing command-line arguments to configure the application.
//! - **`dotenv`**: For loading environment variables from a `.env` file.
//! - **`chrono`**: Handles date and time operations, useful for timestamping metrics
//! - **`serde_json**: Serializes and deserializes JSON data
//!
//! These dependencies are stable and widely used, aligning with the guideline to minimize
//! external dependencies while enhancing functionality.
//!
//! ## Usage
//! 1. Ensure a PostgreSQL database is running (e.g., database `tor_metrics`, user `postgres`,
//!    password `2099`).
//! 2. Configure the application using either a `.env` file or command-line arguments:
//!    - **Using a `.env` file**: Create a `.env` file in the project root with:
//!      ```env
//!      BASE_URL=https://collector.torproject.org
//!      DIRS=recent/relay-descriptors/consensuses
//!      DB_PARAMS=host=localhost user=postgres password=2099 dbname=tor_metrics
//!      ```
//!    - **Using CLI arguments**: Pass arguments when running the application (see below).
//! 3. Run the application:
//!    ```sh
//!    cargo run -- --base-url https://collector.torproject.org --dirs recent/relay-descriptors/consensuses --db-params "host=localhost user=postgres password=2099 dbname=tor_metrics"
//!    ```
//! 4. Alternatively, set environment variables directly:
//!    ```sh
//!    export BASE_URL=https://collector.torproject.org
//!    export DIRS=recent/relay-descriptors/consensuses
//!    export DB_PARAMS="host=localhost user=postgres password=2099 dbname=tor_metrics"
//!    cargo run
//!    ```
//! 5. Logs will be output to the console, controlled by the `RUST_LOG` environment variable:
//!    ```sh
//!    export RUST_LOG=info
//!    cargo run
//!    ```
//!
//! ## Notes
//! - The application uses asynchronous programming with `tokio`, requiring a running async runtime.
//! - Logging levels (e.g., `info`, `debug`, `error`) can be adjusted via the `RUST_LOG` environment variable.

use clap::Parser;
use dotenv::dotenv;
use log::info;
use std::error::Error;
use tor_metrics_mvp::export::export_to_postgres;
use tor_metrics_mvp::fetch::fetch_consensus_files;
use tor_metrics_mvp::parse::parse_consensuses;

/// Command-line arguments for configuring the Tor Metrics MVP application.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
  /// Base URL of the CollecTor instance (e.g., "https://collector.torproject.org").
  #[clap(long, env = "BASE_URL", default_value = "https://collector.torproject.org")]
  base_url: String,

  /// Comma-separated list of directories to fetch from (e.g., "recent/relay-descriptors/consensuses").
  #[clap(long, env = "DIRS", default_value = "recent/relay-descriptors/consensuses", value_delimiter = ',')]
  dirs: Vec<String>,

  /// PostgreSQL connection string (e.g., "host=localhost user=postgres password=your_password dbname=tor_metrics").
  #[clap(long, env = "DB_PARAMS", default_value = "host=localhost user=postgres password=2099 dbname=tor_metrics")]
  db_params: String,

  /// Clear any existing content in the table before exporting
  #[clap(long, action)]
  clear: bool,
}

/// Orchestrates fetching, parsing, and exporting Tor consensus data to PostgreSQL.
///
/// This function:
/// 1. Loads configuration from environment variables or command-line arguments.
/// 2. Fetches consensus files from CollecTor.
/// 3. Parses the files into metrics (e.g., relay counts).
/// 4. Exports the metrics to a PostgreSQL database.
/// 5. Logs progress at each step using the `log` crate.
///
/// # Returns
/// - `Ok(())` if all steps complete successfully.
/// - `Err(Box<dyn Error>)` if any step fails (e.g., network error, database connection issue).
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  // Initialize logging
  env_logger::init();

  // Load environment variables from .env file (if present)
  dotenv().ok();

  // Parse command-line arguments
  let args = Args::parse();
  info!("Starting Tor Metrics MVP with base URL: {}", args.base_url);

    // Fetch consensus files
  let dirs: Vec<&str> = args.dirs.iter().map(|s| s.as_str()).collect();
  let contents = fetch_consensus_files(&args.base_url, &dirs, 0).await?;
  info!("Fetched {} file(s)", contents.len());

  // Parse the fetched files into metrics
  let metrics = parse_consensuses(contents)?;
  info!("Parsed metrics: {:?}", metrics);

  // Export metrics to PostgreSQL
  export_to_postgres(metrics, &args.db_params, args.clear).await?;
  info!("Metrics exported to PostgreSQL");

  Ok(())
}