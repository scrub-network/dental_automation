import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

st.set_page_config(
    page_title="DSO",
    page_icon="🏘️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏘️ DSO")

# Initialize connection
db_uri = st.secrets["db_uri"]
engine = create_engine(db_uri)

# Query data from
dso_practices_sql = text("SELECT * FROM dso_scraping.dso_practices")
dso_df = pd.read_sql(dso_practices_sql, engine)

total_dso_practice = dso_df.shape[0]
total_dso_count = dso_df["dso"].nunique()

c1, c2, c3, c4, c5 = st.columns(5)
with c2: st.metric(label="DSO Count", value=total_dso_count)
with c4: st.metric(label="DSO Practice Count", value=total_dso_practice)

st.write("# ")
st.write("### DSO Practice Count Summary")

# Total count by dso
dso_count_df = dso_df.groupby("dso").agg({"name": "count"}).reset_index()
dso_count_df = dso_count_df.rename(columns={"name": "count"})
dso_count_df = dso_count_df.sort_values("count", ascending=False).reset_index(drop=True)
dso_count_df = dso_count_df.head(10)

# Add total count to the dataframe
dso_count_df.loc[len(dso_count_df)] = ["Total", total_dso_practice]

st.dataframe(dso_count_df, width=1000)

st.data_editor(dso_df, num_rows="dynamic")