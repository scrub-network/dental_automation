import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
import duckdb
from utils.duckdb_update import run_duckdb_updates, get_duckdb_connection
from utils.database_path import get_database_path

st.set_page_config(
    page_title="New Mapping",
    page_icon=":lock with key:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🔐 New Mapping")

# Initialize DuckDB connection
duckdb_conn = duckdb.connect(database=get_database_path())

# Query data from DuckDB
mapping_df = duckdb_conn.execute("SELECT * FROM job_title_mapping").df()
dso_df = duckdb_conn.execute("SELECT * FROM dso_mapping").df()

tab1, tab2 = st.tabs(["Word Mapping", "DSO Mapping"])

with tab1:
    # View current mapping
    st.dataframe(mapping_df)

    # Add new mapping
    update_type = st.radio("Select an Update Type", ('Add ＋', 'Delete −', 'Update ↺'), horizontal=True)

    if update_type == 'Add ＋':
        new_keyword = st.text_input("New Keyword")
        new_mapping = st.text_input("New Mapping")
    elif update_type == 'Delete −':
        new_keyword = st.selectbox("Select a Key Word", mapping_df['key_word'].unique())
    elif update_type == 'Update ↺':
        original_keyword = st.selectbox("Select a Key Word", mapping_df['key_word'].unique())
        new_keyword = st.text_input("New Key Word")
        new_mapping = st.text_input("New Mapping")

    update_button = st.button("Update ⚙️")

    if update_button and update_type == 'Add ＋':
        new_mapping_df = pd.DataFrame({'key_word': [new_keyword], 'word_mapping': [new_mapping]})
        new_mapping_df.to_sql('job_title_mapping', schema="dental_mapping", con=engine, if_exists='append', index=False)
        st.success(f"Successfully added {new_keyword} to the database! 🎉")

    if update_button and update_type == 'Delete −':
        mapping_df = mapping_df[mapping_df['key_word'] != new_keyword]
        mapping_df.to_sql('job_title_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
        st.success(f"Successfully deleted {new_keyword} from the database! 🎉")

    if update_button and update_type == 'Update ↺':
        mapping_df = mapping_df[mapping_df['key_word'] != original_keyword]
        new_mapping_df = pd.DataFrame({'key_word': [new_keyword], 'word_mapping': [new_mapping]})
        mapping_df = pd.concat([mapping_df, new_mapping_df])
        mapping_df.to_sql('job_title_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
        st.success(f"Successfully updated {original_keyword} to {new_keyword} in the database! 🎉")


with tab2:
    st.dataframe(dso_df)

    with st.expander("View DSO Mapping CSV"):
        # Upload csv file
        uploaded_file = st.file_uploader("Upload a CSV file", type="csv")
        file_submit_button = st.button("Submit")
        if uploaded_file is not None and file_submit_button:
            new_df = pd.read_csv(uploaded_file)
            dso_df = pd.concat([dso_df, new_df])
            dso_df = dso_df.drop_duplicates()
            dso_df.to_sql('dso_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
            st.success(f"Successfully updated DSO mapping in the database! 🎉")

    # Add new mapping
    update_type = st.radio("Select an Update Type", ('Add ＋', 'Delete −', 'Update ↺'), horizontal=True, key="dso_update_type")

    if update_type == 'Add ＋':
        new_keyword = st.text_input("New Keyword", key="dso_new_keyword")
        new_mapping = st.text_input("New Mapping", key="dso_new_mapping")
    elif update_type == 'Delete −':
        new_keyword = st.selectbox("Select a Key Word", dso_df["org_name"].unique(), key="dso_delete_keyword")
    elif update_type == 'Update ↺':
        original_keyword = st.selectbox("Select a Key Word", dso_df["org_name"].unique(), key="dso_update_keyword")
        new_keyword = st.text_input("New Key Word", key="dso_new_keyword_update")
        new_mapping = st.text_input("New Mapping", key="dso_new_mapping_update")

    buttonc1, buttonc2, buttonc3 = st.columns(3)
    with buttonc1:
        update_button = st.button("Update ⚙️", key="dso_update_button")
    with buttonc2:
        reset_button = st.button("Reset 🔄", key="dso_reset_button")

    if update_button and update_type == 'Add ＋':
        new_mapping_df = pd.DataFrame({'org_name': [new_keyword], 'tag': [new_mapping]})
        new_mapping_df.to_sql('dso_mapping', schema="dental_mapping", con=engine, if_exists='append', index=False)
        st.success(f"Successfully added {new_keyword} to the database! 🎉")

    if update_button and update_type == 'Delete −':
        dso_df = dso_df[dso_df["org_name"] != new_keyword]
        dso_df.to_sql('dso_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
        st.success(f"Successfully deleted {new_keyword} from the database! 🎉")

    if update_button and update_type == 'Update ↺':
        dso_df = dso_df[dso_df["org_name"] != original_keyword]
        new_mapping_df = pd.DataFrame({'org_name': [new_keyword], 'tag': [new_mapping]})
        dso_df = pd.concat([dso_df, new_mapping_df])
        dso_df.to_sql('dso_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
        st.success(f"Successfully updated {original_keyword} to {new_keyword} in the database! 🎉")

    if reset_button:
        dso_df = pd.DataFrame({'org_name': [], 'tag': []})
        dso_df.to_sql('dso_mapping', schema="dental_mapping", con=engine, if_exists='replace', index=False)
        st.success(f"Successfully reset DSO mapping! 🎉")
