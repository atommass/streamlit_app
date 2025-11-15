import os
import streamlit as st
import snowflake.connector
import pandas as pd


def _get_secrets():
    # Debug: Check what's available in secrets
    available_keys = []
    try:
        available_keys = list(st.secrets.keys())
    except Exception as e:
        pass
    
    # 1) Prefer explicit `snowflake` section in Streamlit secrets
    try:
        if "snowflake" in st.secrets:
            return dict(st.secrets["snowflake"])
    except Exception as e:
        pass

    # 2) Support a `connections` table like the user's example.
    #    Example supported key: [connections.snowflake]
    try:
        if "connections" in st.secrets:
            conns = st.secrets["connections"]
            # If there's a named `snowflake` connection, use it
            if "snowflake" in conns:
                return dict(conns["snowflake"])
            # Else, if there are any connections defined, return the first one
            if conns:
                first_key = next(iter(conns.keys()))
                return dict(conns[first_key])
    except Exception as e:
        pass

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
    if env.get("user") and env.get("password") and env.get("account"):
        return env

    error_msg = f"Snowflake credentials not found. Available secret keys: {available_keys}. "
    error_msg += "Provide `st.secrets['snowflake']`, `st.secrets['connections']['snowflake']`, or environment variables."
    raise RuntimeError(error_msg)


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
try:
    secrets_available = "snowflake" in st.secrets or "connections" in st.secrets
except Exception:
    secrets_available = False

if not secrets_available:
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
