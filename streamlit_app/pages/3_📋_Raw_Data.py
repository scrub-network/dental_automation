import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from utils.dataframe_filters import add_filters_to_dataframe
from utils.metrics import get_main_metrics

st.set_page_config(
    page_title="Job Postings",
    page_icon=":clipboard:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📋 Job Postings Raw Data")

# Initialize connection
db_uri = st.secrets["db_uri"]
engine = create_engine(db_uri)

job_posting_sql = text("SELECT * FROM public.job_postings")
df = pd.read_sql(job_posting_sql, engine)
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
