import streamlit as st
from utilities import snowflake_connection_helper
from utilities import run_snowflake_query

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
