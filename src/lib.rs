//! # Bridge Pool Assignments Library
//!
//! This library provides functionality to fetch, parse, and export Tor
//! bridge pool assignment documents, mirroring the style of the Tor Project's metrics-lib.
//! 
//! ## Components
//!
//! - **fetch**: Retrieves bridge pool assignment files from a CollecTor instance.
//! - **parse**: Extracts structured data from the raw file content.
//! - **export**: Exports parsed data to a PostgreSQL database.
//! - **utils**: Contains utility functions used across the other modules.
//!
//! ## Digest Calculation
//!
//! Following the original metrics library approach:
//! - **File Digests**: SHA-256 hash of the entire raw file content.
//! - **Assignment Digests**: SHA-256 hash of the raw line bytes combined with the file digest.
//!
//! This approach ensures unique identifiers for both files and assignments in the database schema.

pub mod fetch;
pub mod parse;
pub mod export;
pub mod utils;