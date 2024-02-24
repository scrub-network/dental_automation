import pandas as pd
import streamlit as st
import duckdb
from utils.metrics import get_main_metrics
from utils.duckdb_update import run_duckdb_updates
from utils.database_path import get_database_path

st.set_page_config(
    page_title="Dental Job Board",
    page_icon=":tooth:",
    layout="wide",
)

st.title("Scrub Network")

# Initialize DuckDB connection
duckdb_conn = duckdb.connect(database=get_database_path())

# Query data from DuckDB
df = duckdb_conn.execute("SELECT * FROM job_postings").df()
df = df[['job_title', 'employer', 'location', 'state', 'date_posted', 'job_type', 'description', 'post_link', 'source', 'created_at']]

metric_1, metric_1_delta, metric_2, metric_2_delta, metric_3, metric_3_delta = get_main_metrics(df)

st.write("# ")

c1, c2, c3 = st.columns(3)
with c1:
    st.metric(label="**Job Postings**", value=metric_1, delta=metric_1_delta)
with c2:
    st.metric(label="**Posted Within 7 Days**", value=metric_2, delta=metric_2_delta)
with c3:
    st.metric(label="**Unique Employers**", value=metric_3, delta=metric_3_delta)

st.write("# ")

if st.button("Get Most Updated Data"):
    run_duckdb_updates(manual_trigger=True)
    st.balloons()