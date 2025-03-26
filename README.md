# Bridge Pool Assignments Parser

The `bridge_pool_assignments` project is a Rust application that fetches, parses, and exports Tor bridge pool assignment documents to a PostgreSQL database. It provides a minimal yet functional pipeline for processing Tor bridge pool assignment data, inspired by the structure and style of the Tor Project's `metrics-lib`, adapted for Rust. The application is designed to be efficient, reliable, and easy to use, with a focus on fetching data from a CollecTor instance, parsing it into structured formats, and storing it in a database for further analysis.


## Functionality

The application is organized into four core modules, each handling a distinct part of the data processing pipeline:

- **Fetching (`fetch/`)**  
  This module retrieves bridge pool assignment files from a CollecTor instance (e.g., "https://collector.torproject.org"). It:
  - Fetches the `index.json` file to identify available bridge pool assignment files.
  - Filters files based on specified directories (e.g., "recent/bridge-pool-assignments") and a minimum last-modified timestamp.
  - Downloads file contents concurrently, limiting requests to avoid overwhelming the server (max 50 concurrent fetches).
  - Structures the data into `BridgePoolFile` instances containing the file path, last-modified timestamp, and raw content.
  - **Submodules**: `collector.rs` (contains fetch logic), `types.rs` (defines data structures)

- **Parsing (`parse/`)**  
  This module processes the raw textual content of fetched files into structured data. It:
  - Extracts the publication timestamp from the "bridge-pool-assignment" header line (e.g., "bridge-pool-assignment 2022-04-09 00:29:37").
  - Parses subsequent lines into bridge entries, mapping 40-character hex fingerprints (SHA-1 digests) to assignment strings (e.g., "email transport=obfs4").
  - Returns a vector of `ParsedBridgePoolAssignment` structs, each containing a timestamp and an ordered map of bridge entries.
  - **Submodules**: `bridge_pool.rs` (contains parsing logic), `types.rs` (defines data structures)

- **Exporting (`export/`)**  
  This module saves the parsed data to a PostgreSQL database. It:
  - Establishes a connection to the database using a provided connection string.
  - Creates two tables (`bridge_pool_assignments_file` and `bridge_pool_assignment`) with indexes for performance if they don't exist.
  - Inserts file metadata and bridge entries in a transaction, with batch inserts (1000 rows per batch) for efficiency.
  - Supports an optional `--clear` flag to truncate existing data before insertion.
  - **Submodules**: `postgres.rs` (contains database export functionality)

- **Utilities (`utils/`)**
  This module provides utility functions used throughout the application:
  - Functions for calculating SHA-256 digests for files and assignments.
  - SHA-256 digest calculation for both files and individual assignments.
  - **Submodules**: `digest.rs` (contains digest calculation functions)

The main entry point (`main.rs`) ties these modules together, orchestrating the fetch-parse-export workflow using asynchronous Rust with `tokio`.


## Architecture

The project follows a modular architecture inspired by the original Tor Project's `metrics-lib`:

```
src/
├── fetch/                 # Data fetching functionality
│   ├── mod.rs             # Module interface
│   ├── collector.rs       # Fetch implementation
│   └── types.rs           # Data structures
├── parse/                 # Data parsing functionality
│   ├── mod.rs             # Module interface
│   ├── bridge_pool.rs     # Parser implementation
│   └── types.rs           # Data structures
├── export/                # Database export functionality
│   ├── mod.rs             # Module interface
│   └── postgres.rs        # PostgreSQL export
├── utils/                 # Utility functions
│   ├── mod.rs             # Module interface
│   └── digest.rs          # Digest calculation
├── lib.rs                 # Library interface
└── main.rs                # Application entry point
```

This architecture promotes:
- Clear separation of concerns
- Reusable and testable components
- Ease of maintenance and future extension
- Alignment with the original metrics library approach


## Dependencies

The project uses the following stable and widely-used Rust crates:

- **`reqwest`**: Performs HTTP requests to fetch data from CollecTor.
- **`tokio`**: Provides an asynchronous runtime for network and database operations.
- **`tokio-postgres`**: Manages asynchronous PostgreSQL database interactions.
- **`log` and `env_logger`**: Enables structured logging with configurable levels (e.g., `info`, `debug`).
- **`clap`**: Parses command-line arguments for flexible configuration.
- **`chrono`**: Handles date and time operations, including timestamp parsing and conversion.
- **`serde_json`**: Serializes and deserializes JSON data (e.g., `index.json`).
- **`anyhow`**: Simplifies error handling with detailed context.
- **`sha2`**: Computes SHA-256 digests for file uniqueness.
- **`hex`**: Encodes digests as hexadecimal strings.

These dependencies ensure reliability and maintainability while keeping the project lightweight.


## Usage

To use `bridge_pool_assignments`, follow these steps:

1. **Prerequisites**  
    - **Rust**: Install via [rustup](https://rustup.rs/).
    - **PostgreSQL**: A running server (e.g., version 12 or later).
    - **Git**: For cloning the repository.

2. **Clone the Repository**  
    ```sh
    git clone https://github.com/shivam37483/Tor-GSoC-25.git
    cd Tor-GSoC-25
    ```

3. **Set up a PostgreSQL database**
    ```sql
    CREATE DATABASE dummy_tor_db;
    ```

4. **Update Args Struct of the main.rs with your Postgres's Password**

5. **Control Logging**
    - Windows
      ```sh
      set RUST_LOG=info
      ```

    - Mac/Linux
      ```sh
      export RUST_LOG=info
      ```

    Log levels include error, warn, info, debug, and trace.


6. **Configure and Run**
   
   Run the application with default settings or customise via command-line arguments (See rustdoc comments in the file):

   ```sh
   cargo run -- --clear
   ```

   - --clear: Optional flag to clear existing database tables before exporting.


## Documentation

  - Rustdoc: Generate and view the documentation:
    ```sh
      cargo doc --no-deps --open
    ```

  - Remove <--no-deps> for complete documentation including that for all the dependencies:
    ```sh
      cargo doc --open
    ```

    - Access the HTML output at target/doc/tor_metrics_mvp/index.html.


## Database Schema

The application uses two PostgreSQL tables to store the data:

  - **bridge_pool_assignments_file**
    Stores metadata about each bridge pool assignment file:

      - **published** (TIMESTAMP): Publication timestamp.
      - **header** (TEXT): File header (e.g., "bridge-pool-assignment").
      - **digest** (TEXT, PRIMARY KEY): SHA-256 digest of the file's raw content.
      - Index: **bridge_pool_assignment_file_published** on **published**.

  - **bridge_pool_assignment**
    Stores individual bridge assignments:

      - **digest** (TEXT, PRIMARY KEY): SHA-256 digest calculated from both the raw line bytes and the file digest.
      - **published** (TIMESTAMP): Publication timestamp.
      - **fingerprint** (TEXT): Bridge fingerprint (40-character hex string).
      - **distribution_method** (TEXT): Method of distribution (e.g., "email", "https").
      - **transport** (TEXT, nullable): Transport protocol (e.g., "obfs4").
      - **ip** (TEXT, nullable): IP address.
      - **blocklist** (TEXT, nullable): Blocklist identifier.
      - **bridge_pool_assignments** (TEXT): Foreign key referencing bridge_pool_assignments_file.digest.
      - **distributed** (BOOLEAN): Distribution status (defaults to false).
      - **state** (TEXT, nullable): State information.
      - **bandwidth** (TEXT, nullable): Bandwidth value.
      - **ratio** (REAL, nullable): Ratio value.
      - Indexes: 
          - **bridge_pool_assignment_published** on **published**.
          - **bridge_pool_assignment_fingerprint** on **fingerprint**.
          - **bridge_pool_assignment_fingerprint_published_desc_index** on **(fingerprint, published DESC)**.

## Digest Calculation

The application follows the original Tor metrics library approach for calculating digests:

- **File Digests**: A SHA-256 hash is calculated from the entire raw content of each file. This digest serves as the primary key in the `bridge_pool_assignments_file` table.

- **Assignment Digests**: A SHA-256 hash is calculated from the raw bytes of each individual assignment line combined with the file digest. This digest serves as the primary key in the `bridge_pool_assignment` table.

This approach ensures unique identifiers for both files and individual assignments, even when identical assignments appear in different files. It maintains data integrity, prevents primary key violations, and facilitates proper foreign key relationships between the tables.


## Error Handling

Errors are managed using **anyhow::Result**, which provides detailed context for failures across all modules:
  - **Fetching**: Reports HTTP request failures, JSON parsing errors, or missing files.
  - **Parsing**: Identifies invalid timestamps, malformed bridge entries, or missing headers.
  - **Exporting**: Handles database connection issues, transaction failures, or query execution errors.

Errors are logged using the **log** crate, and users can inspect logs for troubleshooting.


## Testing

The project includes:
  - **Unit Tests**: Verify individual functions in module-specific files (e.g., `fetch/collector.rs`, `parse/bridge_pool.rs`, `utils/digest.rs`).
  - **Doctests**: Embedded in documentation examples to ensure code snippets work as expected.

Run tests with:

```sh
cargo test
```

All tests pass, ensuring the application's core functionality is robust.




