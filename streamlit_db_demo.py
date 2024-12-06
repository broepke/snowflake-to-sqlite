import streamlit as st
from utilities import snowflake_connection_helper
from utilities import run_snowflake_query

st.title("SQLite & Snowflake Intechange Demo")
st.write("This is a small demo application that can take a single query and pass it to either a remote Snowflake warehouse or a local DB cache of tables in SQLlite.")

query = """select * from products where type = 'beverage'"""

st.code(query)

# Query and display the data you inserted
st.subheader("SQL Lite")
sqlite_conn = st.connection("jaffle_shop", type="sql")
sqlite_df = sqlite_conn.query(query)
st.dataframe(sqlite_df)


st.subheader("Snowflake")
snow_conn = snowflake_connection_helper()
snow_df = run_snowflake_query(snow_conn, query)
st.dataframe(snow_df)
