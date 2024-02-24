import pandas as pd
import streamlit as st

def get_main_metrics(df: pd.DataFrame):
    # Job Postings Metrics
    total_job_postings = len(df)

    current_time = pd.Timestamp.now() - pd.Timedelta(hours=3)  # Adjust for 3 hours behind
    last_week = current_time - pd.Timedelta(days=7)
    job_postings_last_week = df[df['date_posted'] <= str(last_week)]
    new_job_postings_last_week = total_job_postings - len(job_postings_last_week)

    # Posted within 7 Days Metrics
    job_postings_last_7_days = len(df[df['date_posted'] >= str(last_week)])
    
    last_week_7_days_ago = current_time - pd.Timedelta(days=14)
    new_job_postings_last_week_7_days = (
        job_postings_last_7_days -
        len(df[(df['date_posted'] >= str(last_week_7_days_ago)) & (df['date_posted'] < str(last_week))])
    )

    # Unique Employers Metrics
    unique_employers = len(df['employer'].unique())

    last_week_df = df[df['date_posted'] <= str(last_week)]
    unique_employers_last_week = len(last_week_df['employer'].unique())
    new_unique_employers_last_week = unique_employers - unique_employers_last_week

    return (
        total_job_postings, new_job_postings_last_week,
        job_postings_last_7_days, new_job_postings_last_week_7_days,
        unique_employers, new_unique_employers_last_week
    )
