import snowflake.connector
import pandas as pd
import streamlit as st

def get_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["SNOWFLAKE_USER"],
        password=st.secrets["snowflake"]["SNOWFLAKE_PASSWORD"],
        account=st.secrets["snowflake"]["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["snowflake"]["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["snowflake"]["SNOWFLAKE_DATABASE"],
        role=st.secrets["snowflake"]["SNOWFLAKE_ROLE"],
        host=st.secrets["snowflake"]["SNOWFLAKE_HOST"]
    )
    return conn

def list_columns(schema_name):
    query = f"""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema_name}'
    ORDER BY table_name, ordinal_position
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df

def sample_column(schema_name, table_name, column_name, n=500):
    query = f"""
    SELECT {column_name}
    FROM {schema_name}.{table_name}
    LIMIT {n}
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df[column_name].astype(str).tolist()
