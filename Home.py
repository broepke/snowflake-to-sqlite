import streamlit as st
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# Function to load the private key from secrets
def load_private_key_from_secrets(private_key_str):
    """Function to take a plain text string of an RSA Private key and convert
       into the proper format needed for a connection string.

    Args:
        private_key_str (str): The original and full RSA key with begin and end strings + line breaks

    Returns:
        RSA Key: binary encoded RSA key from string
    """
    private_key = serialization.load_pem_private_key(
        private_key_str.encode(),
        password=None,
        backend=default_backend(),
    )
    return private_key


@st.cache_resource(ttl=3600)
def snowflake_connection_helper():
    # Load the private key from secrets directly inside the function to avoid passing it as an argument
    private_key = load_private_key_from_secrets(
        st.secrets["connections"]["snowflake"]["private_key"]
    )

    # Set up the connection using the formatted private key, without caching the private key object
    conn = snowflake.connector.connect(
        account=st.secrets["connections"]["snowflake"]["account"],
        user=st.secrets["connections"]["snowflake"]["user"],
        private_key=private_key,
        role=st.secrets["connections"]["snowflake"]["role"],
        warehouse=st.secrets["connections"]["snowflake"]["warehouse"],
        database=st.secrets["connections"]["snowflake"]["database"],
        schema=st.secrets["connections"]["snowflake"]["schema"],
    )

    return conn


def run_snowflake_query(_conn, query):
    with _conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetch_pandas_all()
        return result


query = """select * from picks where year = 2024 and DEATH_DATE is not null"""

# Query and display the data you inserted
st.header("SQL Lite")
sqlite_conn = st.connection("deadpool_db", type="sql")
sqlite_df = sqlite_conn.query(query)
st.dataframe(sqlite_df)


st.header("Snowflake")
snow_conn = snowflake_connection_helper()
snow_df = run_snowflake_query(snow_conn, query)
st.dataframe(snow_df)
