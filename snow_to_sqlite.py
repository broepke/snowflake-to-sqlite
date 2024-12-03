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
    "user": SNOW_USER,
    "password": SNOW_PASS,
    "role": SNOW_ROLE,
    "account": SNOW_ACCOUNT,
    "warehouse": "ENGINEER",
    "database": "DEADPOOL",
    "schema": "PROD",
}

# List of table names to fetch and write
table_names = [
    "picks",
    "players",
    "scores",
    "draft",
    "picks_twenty_four",
    "score_twenty_four",
]

# SQLite database file
sqlite_db_path = "deadpool.db"


def fetch_table_schema(connection, table_name):
    """
    Fetch the schema of a specific table from Snowflake.

    Args:
        connection (snowflake.connector.connection.SnowflakeConnection): Active Snowflake connection
        table_name (str): Name of the table to fetch schema for

    Returns:
        list: List of tuples containing column information (name, type, etc.)

    Example:
        schema = fetch_table_schema(snow_conn, "players")
    """
    query = f"DESCRIBE TABLE {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    schema = cursor.fetchall()
    return schema


def map_snowflake_to_sqlite_type(snowflake_type):
    """
    Map Snowflake data types to their equivalent SQLite data types.

    Args:
        snowflake_type (str): Snowflake data type to convert

    Returns:
        str: Corresponding SQLite data type

    Example:
        sqlite_type = map_snowflake_to_sqlite_type("VARCHAR(255)")  # Returns "TEXT"
        sqlite_type = map_snowflake_to_sqlite_type("NUMBER(10,2)")  # Returns "REAL"
    """
    if "VARCHAR" in snowflake_type or "TEXT" in snowflake_type:
        return "TEXT"
    elif "NUMBER" in snowflake_type:
        return "REAL" if "," in snowflake_type else "INTEGER"
    elif "DATE" in snowflake_type or "TIMESTAMP" in snowflake_type:
        return "TEXT"
    else:
        return "TEXT"  # Default to TEXT for unknown types


def create_table_in_sqlite(table_name, schema):
    """
    Create a table in SQLite based on the Snowflake schema.

    Args:
        table_name (str): Name of the table to create
        schema (list): List of tuples containing column information from Snowflake

    Note:
        - Creates the table if it doesn't exist
        - Automatically maps Snowflake data types to SQLite types
        - Uses the global sqlite_db_path for database connection

    Example:
        create_table_in_sqlite("players", schema_data)
    """
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    columns_with_types = ", ".join(
        [f'"{col[0]}" {map_snowflake_to_sqlite_type(col[1])}' for col in schema]
    )
    create_table_query = (
        f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
    )
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()


def fetch_data_from_snowflake(connection, table_name):
    """
    Fetch all data from a specific table in Snowflake.

    Args:
        connection (snowflake.connector.connection.SnowflakeConnection): Active Snowflake connection
        table_name (str): Name of the table to fetch data from

    Returns:
        tuple: (data, column_names)
            - data: List of tuples containing the table rows
            - column_names: List of column names from the table

    Example:
        data, columns = fetch_data_from_snowflake(snow_conn, "players")
    """
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return data, column_names


def convert_decimal_to_supported_type(data):
    """
    Convert Decimal values to float for SQLite compatibility.

    Args:
        data (list): List of tuples containing row data, possibly with Decimal values

    Returns:
        list: List of tuples with Decimal values converted to float

    Note:
        SQLite doesn't support Decimal type directly, so we convert to float
        for compatibility while maintaining precision.

    Example:
        converted_data = convert_decimal_to_supported_type(raw_data)
    """
    return [
        tuple(float(item) if isinstance(item, Decimal) else item for item in row)
        for row in data
    ]


def write_data_to_sqlite(data, table_name):
    """
    Insert data into the specified SQLite table.

    Args:
        data (list): List of tuples containing the rows to insert
        table_name (str): Name of the target table

    Note:
        - Uses the global sqlite_db_path for database connection
        - Performs bulk insert using executemany for better performance
        - Automatically commits the transaction

    Example:
        write_data_to_sqlite(processed_data, "players")
    """
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Insert data into SQLite
    placeholders = ", ".join(["?" for _ in data[0]])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(insert_query, data)

    conn.commit()
    conn.close()


def main():
    """
    Main function to orchestrate the data transfer from Snowflake to SQLite.

    This function:
    1. Establishes connection to Snowflake
    2. Iterates through predefined table names
    3. For each table:
        - Fetches schema from Snowflake
        - Creates corresponding table in SQLite
        - Transfers data from Snowflake to SQLite
    4. Handles connection cleanup

    Note:
        Uses environment variables for Snowflake credentials and
        global variables for configuration.
    """
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
