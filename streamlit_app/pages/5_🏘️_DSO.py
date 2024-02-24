import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
import duckdb
from utils.duckdb_update import run_duckdb_updates, get_duckdb_connection
from utils.database_path import get_database_path

st.set_page_config(
    page_title="DSO",
    page_icon="🏘️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏘️ DSO")

# Initialize DuckDB connection
duckdb_conn = duckdb.connect(database=get_database_path())

# Query data from DuckDB
dso_df = duckdb_conn.execute("SELECT * FROM dso_practices").df()

total_dso_practice = dso_df.shape[0]
total_dso_count = dso_df["dso"].nunique()

c1, c2, c3, c4, c5 = st.columns(5)
with c2: st.metric(label="DSO Count", value=total_dso_count)
with c4: st.metric(label="DSO Practice Count", value=total_dso_practice)

# Total count by dso
dso_count_df = dso_df.groupby("dso").agg({"name": "count"}).reset_index()
dso_count_df = dso_count_df.rename(columns={"name": "count"})
dso_count_df = dso_count_df.sort_values("count", ascending=False).reset_index(drop=True)
dso_count_df = dso_count_df.head(10)

# Add total count to the dataframe
dso_count_df.loc[len(dso_count_df)] = ["Total", total_dso_practice]

st.dataframe(dso_count_df, width=1000)

st.data_editor(dso_df, num_rows="dynamic")