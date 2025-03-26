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
//! ## Submodules
//!
//! - **postgres**: Contains PostgreSQL-specific export functionality.

mod postgres;

pub use postgres::export_to_postgres; 