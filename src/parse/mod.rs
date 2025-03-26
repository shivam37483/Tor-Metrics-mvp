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
//! ## Submodules
//!
//! - **bridge_pool**: Contains the core parsing logic for bridge pool assignment files.
//! - **types**: Defines data structures used in the parsing process.

mod bridge_pool;
mod types;

pub use bridge_pool::parse_bridge_pool_files;
pub use types::ParsedBridgePoolAssignment; 