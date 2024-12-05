import streamlit as st

# Create the SQL connection to pets_db as specified in your secrets file.
conn = st.connection('deadpool_db', type='sql')

# Query and display the data you inserted
picks = conn.query("""select * from picks where year = 2024""")
st.dataframe(picks)