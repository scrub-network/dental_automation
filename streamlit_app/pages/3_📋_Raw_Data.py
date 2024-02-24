import streamlit as st
import pandas as pd
import duckdb
from utils.dataframe_filters import add_filters_to_dataframe
from utils.metrics import get_main_metrics
from utils.database_path import get_database_path

st.set_page_config(
    page_title="Job Postings",
    page_icon=":clipboard:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📋 Job Postings Raw Data")

# Initialize DuckDB connection
duckdb_conn = duckdb.connect(database=get_database_path())

# Query data from DuckDB
df = duckdb_conn.execute("SELECT * FROM job_postings").df()
df = df[['job_title', 'employer', 'employer_type', 'location', 'state', 'date_posted', 'job_type', 'description', 'post_link', 'source', 'created_at']]

total_job_postings, total_job_postings_delta, total_job_postings_last_7_days, seven_days_ago_delta, total_unique_employers, total_unique_employers_delta = get_main_metrics(df)

st.write("# ")

c1, c2, c3 = st.columns(3)
with c1:
    st.metric(label="**Job Postings**", value=total_job_postings, delta=total_job_postings_delta)
with c2:
    st.metric(label="**Posted Within 7 Days**", value=total_job_postings_last_7_days, delta=seven_days_ago_delta)
with c3:
    st.metric(label="**Unique Employers**", value=total_unique_employers, delta=total_unique_employers_delta)

st.write("# ")
# df = add_filters_to_dataframe(df)
df = st.data_editor(df, num_rows="dynamic")

# Add count of rows
st.write(f"**Total**: {len(df)}")
st.write("# ")
