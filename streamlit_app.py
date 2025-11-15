import os
import streamlit as st
import snowflake.connector
import pandas as pd


def _get_secrets():
    # 1) Prefer explicit `snowflake` section in Streamlit secrets
    if "snowflake" in st.secrets:
        return st.secrets["snowflake"]

    # 2) Support a `connections` table like the user's example.
    #    Example supported key: [connections.snowflake]
    if "connections" in st.secrets:
        conns = st.secrets["connections"]
        # If there's a named `snowflake` connection, use it
        if isinstance(conns, dict) and "snowflake" in conns:
            return conns["snowflake"]
        # Else, if there are any connections defined, return the first one
        if isinstance(conns, dict) and conns:
            return next(iter(conns.values()))

    # 3) Fallback to environment variables (useful for CI or other deploys)
    env = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    if env["user"] and env["password"] and env["account"]:
        return env

    raise RuntimeError(
        "Snowflake credentials not found. Provide `st.secrets['snowflake']`, `st.secrets['connections']['snowflake']`, or environment variables."
    )


def get_connection():
    s = _get_secrets()
    conn = snowflake.connector.connect(
        user=s.get("user"),
        password=s.get("password"),
        account=s.get("account"),
        warehouse=s.get("warehouse"),
        database=s.get("database"),
        schema=s.get("schema"),
        role=s.get("role"),
        insecure_mode=s.get("insecure_mode", False),
    )
    return conn


def run_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query against Snowflake and return a pandas DataFrame.

    Uses `st.secrets['snowflake']` for credentials. Closes connection when done.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        df = cur.fetch_pandas_all()
        return df
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


st.set_page_config(page_title="Americas Addresses Dashboard", layout="wide")

st.title("Americas Address Data from Snowflake")

# Ensure secrets are present (check updated to support both formats)
if "snowflake" not in st.secrets and "connections" not in st.secrets:
    st.error(
        "Snowflake credentials missing. Add them to Streamlit secrets as `[snowflake]` or `[connections.snowflake]`."
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
