# Tor Metrics MVP

**Tor Metrics MVP** is a Rust-based application that fetches, parses, and exports Tor network consensus documents to a PostgreSQL database. This project serves as a minimal viable product (MVP) to demonstrate a pipeline for processing Tor consensus data, inspired by the Tor Project's `metrics-lib` (<a href="https://gitlab.torproject.org/tpo/network-health/metrics/library"> link </a>), but reimagined in Rust for performance, modern development practices, and a lightweight footprint.

The application retrieves consensus documents from CollecTor, extracts key metrics (e.g., relay counts), and stores the results in a PostgreSQL database, facilitating statistical analysis of the Tor network or the development of related services.

## Features

- **Fetching**: Retrieves consensus documents from the CollecTor service asynchronously.
- **Parsing**: Extracts metrics such as relay counts and timestamps from consensus data.
- **Exporting**: Stores parsed metrics in a PostgreSQL database, with an option to clear existing data.
- **Configuration Flexibility**: Supports configuration via command-line arguments, environment variables, or a `.env` file.
- **Comprehensive Documentation**: Includes thorough comments to generate detailed Rustdoc documentation, enhancing code readability and maintainability.
- **Key Highlights**: Follows the indentation, <a href="https://gitlab.torproject.org/shivam37483/tor-metrics/-/blob/master/CONTRIB.md?ref_type=heads"> coding guidelines </a> stated in the official tor-metrics lib.
  


## Getting Started

### Prerequisites

- **Rust**: Install via [rustup](https://rustup.rs/).
- **PostgreSQL**: A running server (e.g., version 12 or later).
- **Git**: For cloning the repository.

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-username/tor_metrics_mvp.git
   cd tor_metrics_mvp
   ```

2. Install dependencies:
   ```sh
   cargo build
   ```

3. Set up a PostgreSQL database:
   ```sql
   CREATE DATABASE tor_metrics;
   ```

4. Create a user (e.g., postgres) with a password (e.g., 2099):
     ```sql
     CREATE USER postgres WITH PASSWORD '2099';
     GRANT ALL PRIVILEGES ON DATABASE tor_metrics TO postgres;
     ```

5. Create the tor_metrics table:
      ```sql
       CREATE TABLE tor_metrics (
         id SERIAL PRIMARY KEY,
         metric_name TEXT NOT NULL,
         metric_value BIGINT NOT NULL,
         timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
       );
     ```

### Running the Application

1. Build and run with default settings from .env
  ```sh
    cargo run -- --clear
  ```

2. Use command-line arguments to override defaults and give custom input:
   ```sh
   cargo run -- --base-url https://collector.torproject.org --dirs recent/relay-descriptors/consensuses --db-params "host=localhost user=postgres password=2099 dbname=tor_metrics" --clear
   ```

3. Enable logging (set log level to info)
  ```sh
  set RUST_LOG=info
  cargo run -- --clear
  ```

### Verifying the Output

  - Query the database using pgAdmin or a similar tool
    ```sql
      SELECT * FROM tor_metrics ORDER BY timestamp DESC;
    ```

  - Monitor logs in the terminal for progress (e.g., "Fetched X file(s)", "Metrics exported to PostgreSQL").

### Documentation

  - Rustdoc: Generate and view the documentation:
    ```sh
      cargo doc --no-deps --open
    ```

  - Remove <--no-deps> for complete documentation including that for all the dependencies:
    ```sh
      cargo doc --open
    ```

    - Access the HTML output at target/doc/tor_metrics_mvp/index.html.

