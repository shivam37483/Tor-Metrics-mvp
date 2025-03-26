//! # Utility Functions for Bridge Pool Assignments
//!
//! This module provides general utility functions used throughout the application,
//! including digest calculation functions and other helpers.
//!
//! ## Submodules
//!
//! - **digest**: Contains functions for calculating SHA-256 digests for files and assignments.

mod digest;

pub use digest::{compute_file_digest, compute_assignment_digest}; 