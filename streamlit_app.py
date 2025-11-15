import streamlit as st
import pandas as pd

from snowflake_client import run_query

st.set_page_config(page_title="Americas Addresses Dashboard", layout="wide")

st.title("Americas Address Data from Snowflake")

# Ensure secrets are present
if "snowflake" not in st.secrets:
    st.error(
        "Snowflake credentials missing. Add them to Streamlit secrets as `snowflake` (user, password, account, warehouse, database, schema)."
    )
    st.stop()

# Let user select # of rows to preview
row_limit = st.slider("Rows to display", min_value=10, max_value=1000, value=100, step=10)

# Query editable by the user (optional)
query_default = f"SELECT city, region, address, country FROM v_america LIMIT {row_limit}"
query = st.text_area("SQL Query to Run", query_default, height=120)

if st.button("Run Query"):
    try:
        df = run_query(query)
        if df is None or df.empty:
            st.warning("No results for this query.")
        else:
            st.success(f"Loaded {len(df)} records.")
            st.dataframe(df)
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="v_america.csv",
                mime="text/csv",
            )
    except Exception as ex:
        st.error("Query failed: " + str(ex))
        st.stop()
else:
    # Show preview by default on first load
    try:
        df = run_query(query_default)
        if df is None or df.empty:
            st.info("No preview rows returned.")
        else:
            st.dataframe(df)
    except Exception as ex:
        st.error("Preview query failed: " + str(ex))
