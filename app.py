import streamlit as st
import pandas as pd
from snowflake_utils import list_columns, sample_column
from profiler import generate_column_code
from matcher import final_score
from storage import save_mapping_to_csv, save_signatures, save_mappings

st.title("🔗 ETL Auto Mapper (Snowflake)")

src_schema = st.text_input("Enter Source Schema (Stage):")
tgt_schema = st.text_input("Enter Target Schema:")

if st.button("Run Mapping") and src_schema and tgt_schema:
    st.info("Fetching metadata from Snowflake…")

    src_cols = list_columns(src_schema)
    tgt_cols = list_columns(tgt_schema)

    # --- Source profiling with progress ---
    src_profiles = []
    progress_text = st.empty()
    progress_bar = st.progress(0)

    for i, row in enumerate(src_cols.itertuples(), 1):
        values = sample_column(src_schema, row.TABLE_NAME, row.COLUMN_NAME)
        code, profile = generate_column_code(values)
        src_profiles.append({
            "col_name": row.COLUMN_NAME,
            "table_name": row.TABLE_NAME,
            "code": code,
            "profile": profile
        })
        progress_text.text(f"Profiling Source: {i}/{len(src_cols)} columns")
        progress_bar.progress(i / len(src_cols))

    progress_text.text("✅ Source schema profiling complete!")
    save_signatures(src_schema, src_profiles, created_by="Aashish")

    # --- Target profiling with progress ---
    tgt_profiles = []
    progress_text = st.empty()
    progress_bar = st.progress(0)

    for i, row in enumerate(tgt_cols.itertuples(), 1):
        values = sample_column(tgt_schema, row.TABLE_NAME, row.COLUMN_NAME)
        code, profile = generate_column_code(values)
        tgt_profiles.append({
            "col_name": row.COLUMN_NAME,
            "table_name": row.TABLE_NAME,
            "code": code,
            "profile": profile
        })
        progress_text.text(f"Profiling Target: {i}/{len(tgt_cols)} columns")
        progress_bar.progress(i / len(tgt_cols))

    progress_text.text("✅ Target schema profiling complete!")
    save_signatures(tgt_schema, tgt_profiles, created_by="Aashish")

    # --- Matching with progress ---
    results = []
    progress_text = st.empty()
    progress_bar = st.progress(0)

    for i, s in enumerate(src_profiles, 1):
        best_match, best_score, best_table, best_breakdown = None, 0, None, {}

        for t in tgt_profiles:
            score, breakdown = final_score(s, t)  # unpack tuple
            if score > best_score:
                best_score = score
                best_match = t["col_name"]
                best_table = t["table_name"]
                best_breakdown = breakdown

        results.append({
            "Source Table": s["table_name"],
            "Source Column": s["col_name"],
            "Best Target Table": best_table,
            "Best Target Column": best_match,
            "Score": round(best_score, 3),
            "Name_Score": best_breakdown.get("name_score", None),
            "Profile_Score": best_breakdown.get("profile_score", None),
            "Embed_Score": best_breakdown.get("embed_score", None),
            "Reason": best_breakdown.get("reason", "")
        })

        progress_text.text(f"Matching columns: {i}/{len(src_profiles)} processed")
        progress_bar.progress(i / len(src_profiles))

    progress_text.text("✅ Column matching complete!")
    save_mappings(src_schema, tgt_schema, results, created_by="Aashish")

    # --- Show Results ---
    st.subheader("Mapping Results")
    df = pd.DataFrame(results)
    st.dataframe(df)

    st.download_button("Download Mapping CSV", df.to_csv(index=False), "mapping.csv")
    save_mapping_to_csv(results)
