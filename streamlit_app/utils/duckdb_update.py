import pandas as pd
import duckdb
import sqlalchemy
from sqlalchemy import create_engine
from utils.database_path import get_database_path
import streamlit as st
import os

def get_source_database_url():
    # Database credentials for the source database
    cwd = os.getcwd()
    if 'Volume' in cwd:
        return "postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs"
    else:
        return "postgresql://postgres:postgres@dental_postgres_db:5432/dental_jobs"

@st.cache_resource
def get_duckdb_connection():
    return duckdb.connect(database=get_database_path())

def load_data_to_duckdb(duckdb_conn, engine, table_name, query, manual_trigger=False):
    """
    Load data from the source database to DuckDB for a given table.
    """
    try:
        # Check if the table exists and has data
        count = duckdb_conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    except duckdb.CatalogException:
        count = 0

    if count == 0 or manual_trigger:
        df = pd.read_sql_query(query, engine)
        duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

def run_duckdb_updates(manual_trigger=False):
    # Create streamlit bar prorgress
    my_bar = st.progress(0, "Requesting data from the Main DB")
    duckdb_conn = get_duckdb_connection()
    engine = create_engine(get_source_database_url())
    my_bar.progress(5, "Updating Job Postings Data")
    load_data_to_duckdb(duckdb_conn, engine, 'job_postings', "SELECT * FROM job_postings", manual_trigger)
    my_bar.progress(10, "Updating Private Practices Data")
    load_data_to_duckdb(duckdb_conn, engine, 'source_private_practices', "SELECT * FROM source.source_private_practices", manual_trigger)
    my_bar.progress(20, "Updating Job Title Mapping Data")
    load_data_to_duckdb(duckdb_conn, engine, 'job_title_mapping', "SELECT * FROM dental_mapping.job_title_mapping", manual_trigger)
    my_bar.progress(40, "Updating DSO Mapping Data")
    load_data_to_duckdb(duckdb_conn, engine, 'dso_mapping', "SELECT * FROM dental_mapping.dso_mapping", manual_trigger)
    my_bar.progress(50, "Updating Regional Search Practices Data")
    load_data_to_duckdb(duckdb_conn, engine, 'regional_search_practices', "SELECT * FROM dental_practices.regional_search_practices", manual_trigger)
    my_bar.progress(70, "Updating DSO Data")
    load_data_to_duckdb(duckdb_conn, engine, 'dso_practices', "SELECT * FROM dso_scraping.dso_practices", manual_trigger)
    my_bar.progress(90, "Organizing DSO Data")
    load_data_to_duckdb(duckdb_conn, engine, 'dso', "SELECT * FROM dental_practices.dso", manual_trigger)
    my_bar.progress(100, "Update Completed")
    print("Data loading to DuckDB completed.")

if __name__ == "__main__":
    run_duckdb_updates()