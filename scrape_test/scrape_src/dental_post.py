import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
import random
import uuid
import openai
import us
from utils.us_states_mapping import get_full_state_name
from sqlalchemy import create_engine, text

class DentalPost:
    def __init__(self):
        s = Service(ChromeDriverManager().install())
        # self.driver = webdriver.Chrome(service=s)
        # self.actions = ActionChains(self.driver)
        self.url = "https://www.dentalpost.net/dental-jobs/dentist-jobs/"
        
        self.engine = create_engine('postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs')
        self.existing_df = pd.read_sql_query('select * from source_dental_post',con=self.engine)

    def go_main_page(self):
        self.driver.get(self.url)

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
        col_names = ["job_id", "location", "state", "date_posted", "employer", "job_type", "days_of_week", "description", "post_link", "source", "created_at"]

        # Create an empty dataframe
        jd_df = pd.DataFrame(columns=col_names)
        existing_job_ids = list(self.existing_df['job_id'].values)

        for url in self.website_links:
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

            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            new_row = pd.DataFrame([[job_id, location, state, date_posted, employer, job_type, day_of_week, description, url, "Dental Post", created_at]], columns=col_names)

            jd_df = pd.concat([jd_df, new_row], ignore_index=True)

        return jd_df, self.website_links

    def execute(self):
        all_jobs = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        all_links = []
        page_num = 1

        while page_num < 3:
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