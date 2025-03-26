//! # Fetching Tor Bridge Pool Assignment Files
//!
//! This module provides functionality to fetch bridge pool assignment files from a CollecTor instance
//! (e.g., "https://collector.torproject.org"). It retrieves the `index.json`, filters files based on
//! specified directories (e.g., "bridge_pool_assignments") and a minimum last-modified timestamp,
//! and fetches their contents concurrently. The fetched data is structured into `BridgePoolFile`
//! instances, which can be parsed or directly inserted into a PostgreSQL database.
//!
//! ## Usage
//!
//! The primary entry point is `fetch_bridge_pool_files`, which takes a base URL, a list of directories,
//! and a minimum last-modified timestamp to filter files.
//!
//! ## Submodules
//!
//! - **collector**: Contains the logic for fetching data from a CollecTor instance.
//! - **types**: Defines data structures used in the fetching process.

mod collector;
mod types;

pub use collector::fetch_bridge_pool_files;
pub use types::BridgePoolFile; 