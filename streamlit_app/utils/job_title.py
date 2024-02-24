import re
from sqlalchemy import create_engine, text
import pandas as pd

def generate_job_title(job_title_str, mapping_df):

    # NOTE: The job titles are replaced with the url if there is no job title
    if 'https' in job_title_str:
        job_title_str = job_title_str.split('/')[-1]
        job_title_str = job_title_str.replace('-', ' ')

        for index, row in mapping_df.iterrows():
            if row['key_word'] in job_title_str.lower():
                return row['word_mapping']
            elif 'denti' in job_title_str.lower():
                return 'Dentist'
            else:
                return job_title_str.title()
    else:
        return job_title_str.title()
