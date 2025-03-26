//! Bridge Pool Assignments Parser: Fetch, Parse, and Export Bridge Pool Assignment Documents to PostgreSQL
//!
//! This application demonstrates fetching Tor network bridge pool assignment documents from CollecTor,
//! parsing their contents into structured data, and exporting the results to a PostgreSQL database.
//! It mirrors the style and structure of the Tor Project's `metrics-lib`, adapted for Rust.
//!
//! ## Purpose
//! The goal is to showcase a minimal yet functional pipeline for processing Tor bridge pool
//! assignment data in Rust, with the final step being storage in a PostgreSQL database.
//!
//! ## Design Overview
//! - **Fetching**: Retrieves bridge pool assignment files from CollecTor using the `fetch` module.
//! - **Parsing**: Extracts structured data (e.g., bridge assignments) using the `parse` module.
//! - **Exporting**: Saves parsed data to a PostgreSQL database via the `export` module.
//! - **Utilities**: Provides digest calculation and other utilities via the `utils` module.
//!
//! ## Architecture
//! The application follows a modular structure that mirrors the original metrics-lib:
//! - Each module has its own directory with specialized submodules
//! - Types are separated from their implementation
//! - Common utilities are factored out into a dedicated module
//! - Clear separation of concerns with minimal public interfaces
//!
//! ## Dependencies
//! - **`reqwest`**: For HTTP requests to fetch data from CollecTor.
//! - **`tokio`**: For asynchronous runtime to handle network and database operations.
//! - **`tokio-postgres`**: For PostgreSQL database interaction.
//! - **`log` and `env_logger`**: For structured logging instead of `println!`.
//! - **`clap`**: For parsing command-line arguments to configure the application.
//! - **`chrono`**: Handles date and time operations, useful for timestamping metrics.
//! - **`serde_json`**: Serializes and deserializes JSON data.
//! - **`anyhow`**: For robust error handling across the application.
//! - **`futures`**: For working with asynchronous operations and futures.
//! - **`sha2`**: For computing SHA-2 hashes, ensuring data integrity.
//! - **`hex`**: For encoding and decoding hexadecimal strings, used with hashes.
//! 
//! These dependencies are stable and widely used, aligning with the guideline to minimize
//! external dependencies while enhancing functionality.
//!
//! ## Usage
//! 1. Ensure a PostgreSQL database is running and accessible.
//! 2. Customize the application by passing command-line arguments (see below) if required.
//! 3. Run the application with a tailored database connection string:
//!    ```sh
//!    cargo run -- --base-url https://collector.torproject.org --dirs recent/bridge-pool-assignments --db-params "host=localhost user=your_user password=your_password dbname=your_db"
//!    ```
//! 4. Logs will be output to the console, controlled by the `RUST_LOG` environment variable:
//!    - For Windows:
//!      ```sh
//!      set RUST_LOG=info
//!      cargo run
//!      ```
//!    - For Mac/Linux:
//!      ```sh
//!      export RUST_LOG=info
//!      cargo run
//!      ```
//!
//! ## Notes
//! - The application uses asynchronous programming with `tokio`, requiring a running async runtime.
//! - Logging levels (e.g., `info`, `debug`, `error`) can be adjusted via the `RUST_LOG` environment variable.
//! - The database connection string should be customized to match your PostgreSQL setup.

use clap::Parser;
use log::info;
use std::error::Error;
use bridge_pool_assignments::export::export_to_postgres;
use bridge_pool_assignments::fetch::fetch_bridge_pool_files;
use bridge_pool_assignments::parse::parse_bridge_pool_files;

/// Command-line arguments for configuring the Tor Metrics MVP application.
///
/// This struct defines the options users can provide to customize the application's behavior,
/// such as the CollecTor URL, directories to fetch, and database connection details.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
  /// Base URL of the CollecTor instance to fetch data from.
  ///
  /// Example: "https://collector.torproject.org"
  #[clap(long, env = "BASE_URL", default_value = "https://collector.torproject.org")]
  base_url: String,

  /// Comma-separated list of directories to fetch bridge pool assignment files from.
  ///
  /// Example: "recent/bridge-pool-assignments"
  #[clap(long, env = "DIRS", default_value = "recent/bridge-pool-assignments", value_delimiter = ',')]
  dirs: Vec<String>,

  /// PostgreSQL connection string specifying database access details.
  ///
  /// Example: "host=localhost user=your_user password=your_password dbname=your_db"
  #[clap(long, env = "DB_PARAMS", default_value = "host=localhost user=postgres password=<your_password> dbname=dummy_tor_db")]
  db_params: String,

  /// If set, clears any existing content in the database table before exporting new data.
  #[clap(long, action)]
  clear: bool,
}

/// Entry point for the Tor Metrics MVP application.
///
/// This function orchestrates the core workflow:
/// 1. Initializes logging using `env_logger`.
/// 2. Parses command-line arguments into the `Args` struct.
/// 3. Fetches bridge pool assignment files from CollecTor.
/// 4. Parses the fetched files into structured data (e.g., bridge assignments).
/// 5. Exports the parsed data to a PostgreSQL database.
/// 6. Logs progress at each step using the `log` crate.
///
/// ## Digest Calculation
/// Following the maintainer's recommendations and the original implementation:
/// - For files: SHA-256 hash of the entire raw file content
/// - For individual assignments: SHA-256 hash of each raw line's bytes combined with the file digest
/// 
/// This approach ensures unique digests for both tables, matching the expected schema and
/// preventing duplicate key violations when identical assignments appear in different files.
///
/// # Returns
/// - `Ok(())` if the entire workflow completes successfully.
/// - `Err(Box<dyn Error>)` if an error occurs (e.g., network failure, database connection issue).
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  // Initialize logging with more verbose configuration
  env_logger::Builder::new()
    .format_timestamp(Some(env_logger::TimestampPrecision::Seconds))
    .format_module_path(false)
    .format_level(true)
    .filter_level(log::LevelFilter::Info) // Default to info level if RUST_LOG not set
    .parse_env("RUST_LOG") // Still respect RUST_LOG env var if set
    .init();

  // Print confirmation of logger initialization
  log::info!("Logger initialized at level: {}", std::env::var("RUST_LOG").unwrap_or_else(|_| "INFO".to_string()));

  // Parse command-line arguments
  let args = Args::parse();
  info!("Starting Bridge Pool Assignments Parser with base URL: {}", args.base_url);

  // Fetch bridge pool assignment files
  info!("Starting to fetch the files");
  let dirs: Vec<&str> = args.dirs.iter().map(|s| s.as_str()).collect();
  let contents = fetch_bridge_pool_files(&args.base_url, &dirs, 0).await?;
  info!("Fetched {} file(s)", contents.len());

  // Parse the fetched files into structured data
  info!("Starting to parse the files");
  let parsed_data = parse_bridge_pool_files(contents)?;
  info!("Parsed {} bridge pool assignments", parsed_data.len());

  // Export parsed data to PostgreSQL
  info!("Starting export to PostgreSQL");
  export_to_postgres(parsed_data, &args.db_params, args.clear).await?;
  info!("Bridge pool assignments exported to PostgreSQL");

  Ok(())
}