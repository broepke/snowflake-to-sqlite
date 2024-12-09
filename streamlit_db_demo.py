import streamlit as st
import altair as alt
from utilities import snowflake_connection_helper
from utilities import run_snowflake_query

st.title("SQLite & Snowflake Intechange Demo")
st.write(
    "This is a small demo application that can take a single query and pass it to either a remote Snowflake warehouse or a local DB cache of tables in SQLlite."
)

query = """
SELECT
	C.NAME,
	SUM(O.ORDER_TOTAL) AS TOTAL_ORDERS
FROM CUSTOMERS AS C
JOIN ORDERS AS O
ON C.ID = O.CUSTOMER
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5
"""

st.code(query)

# Query and display the data you inserted
st.subheader("SQL Lite")
sqlite_conn = st.connection("jaffle_shop", type="sql")
sqlite_df = sqlite_conn.query(query.lower())
sqlite_df = sqlite_df.sort_values(by="total_orders", ascending=False)

st.dataframe(sqlite_df)

sqllite_orders_chart = (
    alt.Chart(sqlite_df)
    .mark_bar()
    .encode(
        x="NAME",
        y=alt.Y("total_orders", sort=None),
        color=alt.value("#F63366"),
    )
)

st.altair_chart(sqllite_orders_chart, use_container_width=True)


st.subheader("Snowflake")
snow_conn = snowflake_connection_helper()
snow_df = run_snowflake_query(snow_conn, query)
snow_df = snow_df.sort_values(by="TOTAL_ORDERS", ascending=False)

st.dataframe(snow_df)

snow_orders_chart = (
    alt.Chart(snow_df)
    .mark_bar()
    .encode(
        x="NAME",
        y=alt.Y("TOTAL_ORDERS", sort=None),
        color=alt.value("#F63366"),
    )
)

st.altair_chart(snow_orders_chart, use_container_width=True)
