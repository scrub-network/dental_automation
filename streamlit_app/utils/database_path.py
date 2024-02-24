import os

def get_database_path():
    # Get the current working directory
    cwd = os.getcwd()

    # Check if the cwd is for production or local environment and set the path
    if 'Volume' in cwd:
        return 'streamlit_app/data/duckdb_data.db'
    else:
        return 'data/duckdb_data.db'


