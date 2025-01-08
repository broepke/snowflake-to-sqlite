# Snowflake to SQLite/DuckDB

A Python utility that transfers data from Snowflake tables to a local SQLite or DuckDB database. This tool automatically handles schema mapping and data type conversions between Snowflake and your chosen target database.

For some fun facts on SQLite, check out this [thread on X](https://x.com/iavins/status/1873382770203844884?s=46)!

## Features

- Automatic schema conversion from Snowflake to SQLite or DuckDB
- Bulk data transfer with type handling
- Configurable table selection
- Environment-based configuration for security
- Choice between SQLite and DuckDB as target databases

## Prerequisites

- Python 3.x
- Access to a Snowflake account with appropriate permissions
- Required environment variables set up for Snowflake authentication

## Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/snowflake-to-sqlite-duckdb.git
cd snowflake-to-sqlite-duckdb
```

Install required dependencies:

```bash
pip install -r requirements.txt
```

## Data

In the data folder is a set of CSV files that can be loaded into your data warehouse for testing this if you'd like. These files come from the dbt Demo project "Jaffle Shop"

[https://github.com/dbt-labs/jaffle-shop/](https://github.com/dbt-labs/jaffle-shop/)

## Configuration

Set up your `.secrets.toml` file to connect to your Snowflake instance. You can learn more here if needed [Connect Streamlit to Snowflake](https://docs.streamlit.io/develop/tutorials/databases/snowflake)

```toml
[connections.jaffle_shop]
url = "sqlite:///jaffle_shop.db"

[connections.duckdb]
url = "duckdb:///jaffle_shop.duckdb?read_only=false"

[connections.snowflake]
account = ""
user = ""
private_key = """
-----BEGIN PRIVATE KEY-----
<<paste your key here>>
-----END PRIVATE KEY-----
"""
role = ""
warehouse = ""
database = ""
schema = ""
client_session_keep_alive = true
```

To modify the tables to export, edit either `snow_to_sqlite.py` or `snow_to_duckdb.py`:

```python
table_names = [
    "customers",
    "orders",
    "products",
    "stores",
    "supplies",
]
```

## Usage

Run either script depending on your preferred target database:

For SQLite:

```bash
python snow_to_sqlite.py
```

For DuckDB:

```bash
python snow_to_duckdb.py
```

The scripts will:

1. Connect to your Snowflake instance
2. Create a local database (`jaffle_shop.db` for SQLite or `jaffle_shop.duckdb` for DuckDB)
3. Transfer the configured tables
4. Handle data type conversions automatically

## Output

The script creates either:

- A SQLite database file named `jaffle_shop.db`, or
- A DuckDB database file named `jaffle_shop.duckdb`

in the same directory. The chosen database will contain all the transferred tables with their data and appropriate schema mappings.

## Data Type Mappings

The scripts automatically map Snowflake data types to appropriate types in the target database:

### SQLite Mappings

- VARCHAR/TEXT → TEXT
- NUMBER (with decimals) → REAL
- NUMBER (without decimals) → INTEGER
- DATE/TIMESTAMP → TEXT
- Other types → TEXT

### DuckDB Mappings

- VARCHAR/TEXT → VARCHAR
- NUMBER (with decimals) → DOUBLE
- NUMBER (without decimals) → BIGINT
- DATE → DATE
- TIMESTAMP → TIMESTAMP
- Other types → VARCHAR

## Error Handling

The scripts include error handling for:

- Connection issues
- Data type conversions
- Schema mismatches

If any errors occur during execution, they will be printed to the console with relevant details.
