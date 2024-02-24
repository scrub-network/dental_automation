import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import uuid
import openai
from utils.us_states_mapping import get_full_state_name
from utils.dso_mapping import generate_dso_mapping
from sqlalchemy import create_engine
from airflow.models import Variable

class DentalPost:
    def __init__(self):
        self.url = "https://www.dentalpost.net/dental-jobs/dentist-jobs/"        
        db_uri = db_uri = "postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs"
        self.engine = create_engine(db_uri)
        self.existing_df = pd.read_sql_query('select * from source_dental_post',con=self.engine)
        self.dso_mapping = pd.read_sql_query('select * from dental_mapping.dso_mapping', con=self.engine)

    def scrape_main_page(self, page_num=1):
        
        if page_num > 1:
            self.url = "https://www.dentalpost.net/dental-jobs/dentist-jobs/" + f"{page_num}/"
        print("Updated URL ", self.url)
        html_content = requests.get(self.url).text
        soup = BeautifulSoup(html_content, "lxml")
        
        a_tags = soup.find_all('a')

        # Extract href attributes from each <a> tag
        self.website_links = ['https://www.dentalpost.net' + str(tag['href']) 
                for tag in a_tags if tag.has_attr('href') and '/dental-jobs/job-post' in tag['href']]

    def extract_company_name(self, job_description):

        openai.api_key = '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'

        # Create the prompt
        prompt = f"From the following job description, please extract and provide the name of the company:\n{job_description}"

        # Make the API call
        response = openai.Completion.create(
            model="text-davinci-003",
            prompt=prompt,
            max_tokens=13  # Limit the response length
        )

        # Extract the company name from the response
        company_name = response.choices[0].text.strip()
        company_name = company_name.strip("Answer:\n")
        
        return company_name

    def scrape_each_page(self):

        # Column names
        col_names = ["job_id", "location", "state", "date_posted", "employer", "employer_type", "job_type", "days_of_week", "description", "post_link", "source", "created_at"]

        # Create an empty dataframe
        jd_df = pd.DataFrame(columns=col_names)
        existing_job_ids = list(self.existing_df['job_id'].values)

        for url in self.website_links:

            # Get rid of / at the end of the url
            url = url.rstrip('/')

            # create a uuid given the url
            job_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
            if job_id in existing_job_ids:
                print("DENTAL POST: Skipping ", url)
                continue

            print("DENTAL POST: Reading ", url)
            html_content = requests.get(url).text
            soup = BeautifulSoup(html_content, "lxml")

            # Get class="mb-3 card"
            card = soup.find('div', class_="mb-3 card")
            location = soup.select_one('h3.h6:contains("Location") + p').text.strip()
            state = get_full_state_name(location)
            date_posted = soup.select_one('h4.h6:contains("DATE POSTED") + p').text.strip()
            date_posted = datetime.strptime(date_posted, '%b %d, %Y').strftime('%Y-%m-%d')
            try:
                job_type = soup.select_one('h4.h6:contains("JOB TYPE") + p').text.strip()
            except AttributeError:
                job_type = 'Undefined'
            try:
                day_of_week = soup.select_one('h4.h6:contains("DAYS OF WEEK") + p').text.strip()
            except AttributeError:
                day_of_week = 'Undefined'
            try:
                description = soup.select_one('h3.h6:contains("Description") + p').text.strip()
            except AttributeError:
                description = 'Undefined'
            try:
                employer = soup.select_one('h4.h6:contains("EMPLOYER") + div > p').text.strip()
            except AttributeError:
                employer = self.extract_company_name(description) + " (AI)"
            if employer == "Not provided":
                employer = self.extract_company_name(description) + " (AI)"

            # DSO Mapping
            employer_type = generate_dso_mapping(employer, self.dso_mapping)

            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            new_row = pd.DataFrame([[job_id, location, state, date_posted, employer, employer_type, job_type, day_of_week, description, url, "Dental Post", created_at]], columns=col_names)

            jd_df = pd.concat([jd_df, new_row], ignore_index=True)

        return jd_df, self.website_links

    def execute(self):
        all_jobs = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "employer_type", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        all_links = []
        page_num = 1

        while page_num < 4:
            print("=" * 50)
            print("Reading Page ", page_num)

            # Scrape main page and each pages
            self.scrape_main_page(page_num)
            jd_df, website_links = self.scrape_each_page()

            # Concatenate all jobs and links
            all_jobs = pd.concat([all_jobs, jd_df], ignore_index=True)
            all_links += website_links
            page_num += 1

        all_jobs.to_sql('source_dental_post', con=self.engine, if_exists='append', index=False)

if __name__ == "__main__":
    dp = DentalPost()
    dp.execute()