# Snowflake to SQLite

A Python utility that transfers data from Snowflake tables to a local SQLite database. This tool automatically handles schema mapping and data type conversions between Snowflake and SQLite.

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

[https://github.com/dbt-labs/jaffle-shop/tree/main](https://github.com/dbt-labs/jaffle-shop/tree/main)

## Configuration

Set up the following environment variables with your Snowflake credentials:

```bash
export SNOW_USER="your_snowflake_username"
export SNOW_PASS="your_snowflake_password"
export SNOW_ACCOUNT="your_snowflake_account"
export SNOW_ROLE="your_snowflake_role"
```

The script is configured to use:
- Warehouse: ENGINEER
- Database: DEADPOOL
- Schema: PROD

To modify these settings or the tables to transfer, edit the configuration variables in `snow_to_sqlite.py`:

```python
snowflake_conn_params = {
    "warehouse": "ENGINEER",
    "database": "DEADPOOL",
    "schema": "PROD",
    ...
}

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
2. Create a local SQLite database (`deadpool.db`)
3. Transfer the configured tables
4. Handle data type conversions automatically

## Output

The script creates a SQLite database file named `deadpool.db` in the same directory. This database will contain all the transferred tables with their data and appropriate schema mappings.

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
