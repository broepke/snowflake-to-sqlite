import snowflake.connector
import sqlite3
from decimal import Decimal
import os

# Environment variables for Snowflake credentials
SNOW_USER = os.environ.get("SNOW_USER")
SNOW_PASS = os.environ.get("SNOW_PASS")
SNOW_ACCOUNT = os.environ.get("SNOW_ACCOUNT")
SNOW_ROLE = os.environ.get("SNOW_ROLE")

# Snowflake connection details
snowflake_conn_params = {
    'user': SNOW_USER,
    'password': SNOW_PASS,
    'role': SNOW_ROLE,
    'account': SNOW_ACCOUNT,
    'warehouse': 'ENGINEER',
    'database': 'DEADPOOL',
    'schema': 'PROD',
}

# SQLite database file
sqlite_db_path = "deadpool.db"

def fetch_table_schema(connection, table_name):
    """Fetch the schema of a specific table from Snowflake."""
    query = f"DESCRIBE TABLE {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    schema = cursor.fetchall()
    return schema

def map_snowflake_to_sqlite_type(snowflake_type):
    """Map Snowflake data types to SQLite data types."""
    if "VARCHAR" in snowflake_type or "TEXT" in snowflake_type:
        return "TEXT"
    elif "NUMBER" in snowflake_type:
        return "REAL" if "," in snowflake_type else "INTEGER"
    elif "DATE" in snowflake_type or "TIMESTAMP" in snowflake_type:
        return "TEXT"
    else:
        return "TEXT"  # Default to TEXT for unknown types

def create_table_in_sqlite(table_name, schema):
    """Create a table in SQLite based on the Snowflake schema."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    columns_with_types = ", ".join(
        [f'"{col[0]}" {map_snowflake_to_sqlite_type(col[1])}' for col in schema]
    )
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def fetch_data_from_snowflake(connection, table_name):
    """Fetch data from a specific table in Snowflake."""
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return data, column_names

def convert_decimal_to_supported_type(data):
    """Convert Decimal values to float or string for SQLite compatibility."""
    return [
        tuple(float(item) if isinstance(item, Decimal) else item for item in row)
        for row in data
    ]

def write_data_to_sqlite(data, table_name):
    """Insert data into the specified SQLite table."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Insert data into SQLite
    placeholders = ", ".join(["?" for _ in data[0]])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(insert_query, data)

    conn.commit()
    conn.close()

def main():
    # List of table names to fetch and write
    table_names = ["picks", "players", "scores", "draft", "picks_twenty_four", "score_twenty_four"]

    # Open Snowflake connection
    snowflake_connection = snowflake.connector.connect(**snowflake_conn_params)
    print("Snowflake connection established.")

    try:
        for table_name in table_names:
            print(f"Processing table: {table_name}")
            
            # Fetch Snowflake schema
            schema = fetch_table_schema(snowflake_connection, table_name)
            
            # Create table in SQLite
            create_table_in_sqlite(table_name, schema)
            
            # Fetch data from Snowflake
            data, _ = fetch_data_from_snowflake(snowflake_connection, table_name)
            
            # Convert data types and insert into SQLite
            data = convert_decimal_to_supported_type(data)
            write_data_to_sqlite(data, table_name)
            
            print(f"Data successfully written for table: {table_name}")
    finally:
        # Close Snowflake connection
        snowflake_connection.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    main()