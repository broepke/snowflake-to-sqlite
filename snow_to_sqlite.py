import sqlite3
from decimal import Decimal
from datetime import date, datetime
from utilities import snowflake_connection_helper


# SQLite date/datetime adapters
def adapt_date(val):
    """Convert date to ISO format string."""
    return val.isoformat() if val else None


def adapt_datetime(val):
    """Convert datetime to ISO format string."""
    return val.isoformat() if val else None


def convert_date(val):
    """Convert ISO format string back to date."""
    try:
        return datetime.strptime(val.decode(), "%Y-%m-%d").date() if val else None
    except (ValueError, AttributeError):
        return None


def convert_datetime(val):
    """Convert ISO format string back to datetime."""
    try:
        return datetime.fromisoformat(val.decode()) if val else None
    except (ValueError, AttributeError):
        return None


# Register the adapters and converters
sqlite3.register_adapter(date, adapt_date)
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)


# List of table names to fetch and write
table_names = [
    "customers",
    "items",
    "orders",
    "products",
    "stores",
    "supplies",
]

# SQLite database file
sqlite_db_path = "jaffle_shop.db"


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


def map_snowflake_to_sqlite_type(snowflake_type):
    """
    Map Snowflake data types to their equivalent SQLite data types.

    Args:
        snowflake_type (str): Snowflake data type to convert

    Returns:
        str: Corresponding SQLite data type
    """
    snowflake_type = snowflake_type.upper()

    # Handle VARCHAR types
    if "VARCHAR" in snowflake_type or "TEXT" in snowflake_type:
        return "TEXT"

    # Handle NUMBER types
    elif "NUMBER" in snowflake_type:
        # Check if it's a decimal number
        if "," in snowflake_type:
            return "REAL"
        else:
            return "INTEGER"

    # Handle TIMESTAMP types
    elif "TIMESTAMP_NTZ" in snowflake_type:
        return "datetime"
    elif "TIMESTAMP" in snowflake_type:
        return "datetime"

    # Handle DATE type
    elif "DATE" in snowflake_type:
        return "date"

    # Handle BOOLEAN type
    elif "BOOLEAN" in snowflake_type:
        return "INTEGER"  # SQLite doesn't have native BOOLEAN, use INTEGER (0/1)

    # Default to TEXT for unknown types
    return "TEXT"


def create_table_in_sqlite(table_name, schema):
    """
    Create a table in SQLite based on the Snowflake schema.
    """
    conn = sqlite3.connect(sqlite_db_path, detect_types=sqlite3.PARSE_DECLTYPES)
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


def convert_value(value, snowflake_type):
    """
    Convert a value from Snowflake format to SQLite compatible format.

    Args:
        value: The value to convert
        snowflake_type (str): The Snowflake data type of the value

    Returns:
        The converted value suitable for SQLite storage
    """
    if value is None:
        return None

    snowflake_type = snowflake_type.upper()

    # Handle BOOLEAN
    if "BOOLEAN" in snowflake_type:
        return 1 if value else 0

    # Handle NUMBER(38,0)
    if "NUMBER(38,0)" in snowflake_type:
        return str(value)  # Store as TEXT to preserve large integers

    # Handle other NUMBER types
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


def convert_data_for_sqlite(data, schema):
    """
    Convert all data to SQLite-compatible formats based on schema.

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


def write_data_to_sqlite(data, table_name):
    """
    Insert data into the specified SQLite table.
    """
    conn = sqlite3.connect(sqlite_db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()

    placeholders = ", ".join(["?" for _ in data[0]])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(insert_query, data)

    conn.commit()
    conn.close()


def main():
    """
    Main function to orchestrate the data transfer from Snowflake to SQLite.
    """
    snowflake_connection = snowflake_connection_helper()
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

            # Convert data types based on schema
            converted_data = convert_data_for_sqlite(data, schema)

            # Write to SQLite
            write_data_to_sqlite(converted_data, table_name)

            print(f"Data successfully written for table: {table_name}")
    finally:
        snowflake_connection.close()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    main()
