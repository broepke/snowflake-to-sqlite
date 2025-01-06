import duckdb
from decimal import Decimal
from datetime import date, datetime
from utilities import snowflake_connection_helper

# List of table names to fetch and write
table_names = [
    "customers",
    "items",
    "orders",
    "products",
    "stores",
    "supplies",
]

# DuckDB database file
duckdb_path = "jaffle_shop.duckdb"

def map_snowflake_to_duckdb_type(snowflake_type):
    """
    Map Snowflake data types to their equivalent DuckDB data types.

    Args:
        snowflake_type (str): Snowflake data type to convert

    Returns:
        str: Corresponding DuckDB data type
    """
    snowflake_type = snowflake_type.upper()

    # Handle VARCHAR types
    if "VARCHAR" in snowflake_type or "TEXT" in snowflake_type:
        return "VARCHAR"

    # Handle NUMBER types
    elif "NUMBER" in snowflake_type:
        # Check if it's a decimal number
        if "," in snowflake_type:
            return "DOUBLE"
        else:
            return "BIGINT"

    # Handle TIMESTAMP types
    elif "TIMESTAMP_NTZ" in snowflake_type:
        return "TIMESTAMP"
    elif "TIMESTAMP" in snowflake_type:
        return "TIMESTAMP"

    # Handle DATE type
    elif "DATE" in snowflake_type:
        return "DATE"

    # Handle BOOLEAN type
    elif "BOOLEAN" in snowflake_type:
        return "BOOLEAN"  # DuckDB has native BOOLEAN support

    # Default to VARCHAR for unknown types
    return "VARCHAR"

def fetch_table_schema(connection, table_name):
    """
    Fetch the schema of a specific table from Snowflake.

    Args:
        connection (snowflake.connector.connection.SnowflakeConnection): Active Snowflake connection
        table_name (str): Name of the table to fetch schema for

    Returns:
        list: List of tuples containing column information (name, type, etc.)
    """
    query = f"DESCRIBE TABLE {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    schema = cursor.fetchall()
    return schema

def create_table_in_duckdb(table_name, schema):
    """
    Create a table in DuckDB based on the Snowflake schema.
    """
    conn = duckdb.connect(duckdb_path)
    
    columns_with_types = ", ".join(
        [f'"{col[0]}" {map_snowflake_to_duckdb_type(col[1])}' for col in schema]
    )
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
    conn.execute(create_table_query)
    conn.commit()
    conn.close()

def convert_value(value, snowflake_type):
    """
    Convert a value from Snowflake format to DuckDB compatible format.

    Args:
        value: The value to convert
        snowflake_type (str): The Snowflake data type of the value

    Returns:
        The converted value suitable for DuckDB storage
    """
    if value is None:
        return None

    snowflake_type = snowflake_type.upper()

    # Handle BOOLEAN
    if "BOOLEAN" in snowflake_type:
        return bool(value)  # DuckDB supports native boolean

    # Handle NUMBER types
    if "NUMBER" in snowflake_type:
        if isinstance(value, Decimal):
            if "," in snowflake_type:  # decimal places specified
                return float(value)
            return int(value)

    # Handle TIMESTAMP_NTZ
    if "TIMESTAMP_NTZ" in snowflake_type:
        if isinstance(value, (datetime, date)):
            return value
        # Handle string timestamps
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None

    return value

def fetch_data_from_snowflake(connection, table_name):
    """
    Fetch all data from a specific table in Snowflake.
    """
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return data, column_names

def convert_data_for_duckdb(data, schema):
    """
    Convert all data to DuckDB-compatible formats based on schema.

    Args:
        data (list): List of tuples containing row data
        schema (list): List of tuples containing column information

    Returns:
        list: List of tuples with converted data
    """
    converted_data = []
    for row in data:
        converted_row = []
        for value, column_info in zip(row, schema):
            snowflake_type = column_info[1]
            converted_value = convert_value(value, snowflake_type)
            converted_row.append(converted_value)
        converted_data.append(tuple(converted_row))
    return converted_data

def write_data_to_duckdb(data, table_name, column_names):
    """
    Insert data into the specified DuckDB table.
    """
    import pandas as pd
    
    # Convert data to pandas DataFrame
    df = pd.DataFrame(data, columns=column_names)
    
    conn = duckdb.connect(duckdb_path)
    
    # Create a temporary view from the pandas DataFrame
    conn.register('temp_data', df)
    
    # Insert data from the temporary view into the permanent table
    columns = ', '.join([f'"{col}"' for col in column_names])
    insert_query = f"INSERT INTO {table_name} ({columns}) SELECT * FROM temp_data"
    conn.execute(insert_query)
    
    conn.commit()
    conn.close()

def main():
    """
    Main function to orchestrate the data transfer from Snowflake to DuckDB.
    """
    snowflake_connection = snowflake_connection_helper()
    print("Snowflake connection established.")

    try:
        for table_name in table_names:
            print(f"Processing table: {table_name}")

            # Fetch Snowflake schema
            schema = fetch_table_schema(snowflake_connection, table_name)

            # Create table in DuckDB
            create_table_in_duckdb(table_name, schema)

            # Fetch data from Snowflake
            data, column_names = fetch_data_from_snowflake(snowflake_connection, table_name)

            # Convert data types based on schema
            converted_data = convert_data_for_duckdb(data, schema)

            # Write to DuckDB
            write_data_to_duckdb(converted_data, table_name, column_names)

            print(f"Data successfully written for table: {table_name}")
    finally:
        snowflake_connection.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    main()
