import streamlit as st
import snowflake.connector
import pandas as pd


def _get_secrets():
    if "snowflake" not in st.secrets:
        raise RuntimeError("Snowflake credentials not found in Streamlit secrets. Add them to `st.secrets['snowflake']`.")
    return st.secrets["snowflake"]


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
