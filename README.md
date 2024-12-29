# Snowflake to SQLite

A Python utility that transfers data from Snowflake tables to a local SQLite database. This tool automatically handles schema mapping and data type conversions between Snowflake and SQLite.

For some fun facts on SQLite, check out this [thread on X](https://x.com/iavins/status/1873382770203844884?s=46)!

## Features

- Automatic schema conversion from Snowflake to SQLite
- Bulk data transfer with type handling
- Configurable table selection
- Environment-based configuration for security

## Prerequisites

- Python 3.x
- Access to a Snowflake account with appropriate permissions
- Required environment variables set up for Snowflake authentication

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/snowflake-to-sqlite.git
cd snowflake-to-sqlite
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```
## Data

In the data folder is a set of CSV files that can be loaded into your data warehouse for testing this if you'd like.  These files come from the dbt Demo project "Jaffle Shop" 

[https://github.com/dbt-labs/jaffle-shop/](https://github.com/dbt-labs/jaffle-shop/)

## Configuration

Set up your `.secrets.toml` file to connect to your Snowflake and SQLite instance.  You can learn more here if needed [Connect Streamlit to Snowflake](https://docs.streamlit.io/develop/tutorials/databases/snowflake)

```toml
[connections.jaffle_shop]
url = "sqlite:///jaffle_shop.db"

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

To modify these table to export to SQLite `snow_to_sqlite.py`:

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

Run the script:

```bash
python snow_to_sqlite.py
```

The script will:
1. Connect to your Snowflake instance
2. Create a local SQLite database (`jaffle_shop.db`)
3. Transfer the configured tables
4. Handle data type conversions automatically

## Output

The script creates a SQLite database file named `jaffle_shop.db` in the same directory. This database will contain all the transferred tables with their data and appropriate schema mappings.

## Data Type Mappings

The script automatically maps Snowflake data types to SQLite:
- VARCHAR/TEXT → TEXT
- NUMBER (with decimals) → REAL
- NUMBER (without decimals) → INTEGER
- DATE/TIMESTAMP → TEXT
- Other types → TEXT

## Error Handling

The script includes error handling for:
- Connection issues
- Data type conversions
- Schema mismatches

If any errors occur during execution, they will be printed to the console with relevant details.
