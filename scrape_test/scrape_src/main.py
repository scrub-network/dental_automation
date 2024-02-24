import pandas as pd
from sqlalchemy import create_engine, text
from dental_post import DentalPost
from dentist_job_cafe import DentalJobCafe

class DentalAutomation:
    def __init__(self):
        self.DP = DentalPost()
        self.DJC = DentalJobCafe()
        self.engine = create_engine('postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs')
    
    def scrape_websites(self):
        # Execute Dental Post Automation
        self.dental_post_len = self.DP.execute()
        
        # Execute Dental Job Cafe Automation
        self.dental_job_cafe_len = self.DJC.execute()

    def organize_data(self):
        # Read scraped data from database
        dentist_job_cafe_df = pd.read_sql_query('select * from source_dentist_job_cafe',con=self.engine)
        dental_post_df = pd.read_sql_query('select * from source_dental_post',con=self.engine)

        # Concatenate dataframes
        df = pd.concat([dentist_job_cafe_df, dental_post_df])
        
        # Sort by date posted descending
        df = df.sort_values(by=['date_posted'], ascending=False)

        # Drop duplicates
        df = df.drop_duplicates(subset=['job_id'])

        # Drop rows with empty job_id
        df = df.dropna(subset=['job_id'])

        # Create job_title column by using the url
        df['job_title'] = df['post_link'].apply(lambda x: x.split('/')[-1])
        df['job_title'] = df['job_title'].str.replace('-', ' ')
        df['job_title'] = df['job_title'].str.title()

        df = df[['job_id', 'job_title', 'location', 'state', 'date_posted', 'employer', 'job_type', 'days_of_week', 'description', 'post_link', 'source', 'created_at']]

        df.to_sql('job_postings', con=self.engine, if_exists='replace', index=False)
        print("Successfully updated job_postings table!")

    def execute(self):
        self.scrape_websites()
        self.organize_data()


if __name__ == "__main__":
    dental_automation = DentalAutomation()
    dental_automation.execute()
