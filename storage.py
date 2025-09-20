import pandas as pd
import json
import streamlit as st
import snowflake.connector

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

# Fully qualified table names
TABLE_SIGNATURES = "INSURANCE.DATADNA.ETL_COLUMN_SIGNATURES"
TABLE_MAPPINGS   = "INSURANCE.DATADNA.ETL_COLUMN_MAPPINGS"

# -----------------------------
# Save column signatures
# -----------------------------
def save_signatures(schema_name, profiles, created_by="ETL_MAPPER"):
    insert_sql = f"""
    INSERT INTO {TABLE_SIGNATURES}
    (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, COLUMN_CODE, PROFILE, EMBEDDING, SAMPLE_COUNT, CREATED_AT)
    VALUES (%s, %s, %s, %s, %s, NULL, %s, CURRENT_TIMESTAMP())
    """
    with get_connection() as conn:
        cur = conn.cursor()
        rows = [(schema_name,
                 p["table_name"],
                 p["col_name"],
                 p["code"],
                 json.dumps(p["profile"]),   # plain JSON string
                 p.get("sample_count", 500)) for p in profiles]
        cur.executemany(insert_sql, rows)
        conn.commit()

# -----------------------------
# Save column mappings
# -----------------------------
def save_mappings(src_schema, tgt_schema, results, created_by="ETL_MAPPER"):
    insert_sql = f"""
    INSERT INTO {TABLE_MAPPINGS}
    (SRC_SCHEMA, SRC_TABLE, SRC_COLUMN, TGT_SCHEMA, TGT_TABLE, TGT_COLUMN, SCORE, DECISION, CREATED_BY, CREATED_AT)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'AUTO', %s, CURRENT_TIMESTAMP())
    """
    with get_connection() as conn:
        cur = conn.cursor()
        rows = [(src_schema,
                 r["Source Table"],
                 r["Source Column"],
                 tgt_schema,
                 r["Best Target Table"],
                 r["Best Target Column"],
                 r["Score"],
                 created_by) for r in results]
        cur.executemany(insert_sql, rows)
        conn.commit()

# -----------------------------
# CSV fallback
# -----------------------------
def save_mapping_to_csv(results, filename="mapping.csv"):
    df = pd.DataFrame(results)
    df.to_csv(filename, index=False)
    return filename

def get_signatures(schema_name):
    query = f"""
    SELECT TABLE_NAME, COLUMN_NAME, COLUMN_CODE, PROFILE
    FROM {TABLE_SIGNATURES}
    WHERE SCHEMA_NAME = %s
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (schema_name,))
        rows = cur.fetchall()
    results = []
    for r in rows:
        results.append({
            "table_name": r[0],
            "col_name": r[1],
            "code": r[2],
            "profile": json.loads(r[3]) if r[3] else {}
        })
    return results

def get_mappings(src_schema, tgt_schema):
    query = f"""
    SELECT SRC_TABLE, SRC_COLUMN, TGT_TABLE, TGT_COLUMN, SCORE, DECISION
    FROM {TABLE_MAPPINGS}
    WHERE SRC_SCHEMA = %s AND TGT_SCHEMA = %s
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (src_schema, tgt_schema))
        rows = cur.fetchall()
    results = []
    for r in rows:
        results.append({
            "Source Table": r[0],
            "Source Column": r[1],
            "Best Target Table": r[2],
            "Best Target Column": r[3],
            "Score": r[4],
            "Decision": r[5]
        })
    return results
