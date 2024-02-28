import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import mechanize
import http.cookiejar
import uuid
from utils.us_states_mapping import get_full_state_name
from datetime import datetime
from sqlalchemy import create_engine
import requests
import streamlit as st

class DentalPost:
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs')
        self.existing_df = pd.read_sql_query('select * from source_ihiredental',con=self.engine)

    def log_in(self):
        username = "seanhyang1@gmail.com"
        password = "fifaexecutivedrive%1"
        username_url = "https://www.ihiredental.com/employer/account/signin"
        password_url = "https://www.ihiredental.com/employer/account/password"

        cj = http.cookiejar.CookieJar()
        self.br = mechanize.Browser()
        self.br.set_cookiejar(cj)
        for ind, url in enumerate([username_url, password_url]):
            self.br.open(url)
            self.br.select_form(nr=0)

            if ind == 0:
                self.br.form['Email'] = username
            else:
                self.br.form['Password'] = password
            self.br.submit()

    def scrape_main_page(self, page_num=1):
        self.url = "https://www.ihiredental.com/candidate/jobs/search?k=dentist&loc=&d=50#/search?k=dentist&loc=&d=50&p="
        self.url = self.url + str(page_num) + "&st=page"
        print("Updated URL ", self.url)
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

        html_content = requests.get(self.url, headers=headers).text
        soup = BeautifulSoup(html_content, "lxml")
        
        # Get all items using xpath = '//*[@id="job-411039617"]/div[1]/div/div/div[2]/p/a
        row_divs = soup.find_all('ul', class_="list-unstyled")
        
        print(row_divs)
        # Extract href attributes from each <a> tag
        # self.website_links = ['https://www.dentalpost.net' + str(tag['href']) 
        #          for tag in a_tags if tag.has_attr('href') and '/dental-jobs/job-post' in tag['href']]

    def extract_company_name(self, job_description):

        openai.api_key = st.secrets["openai_api_key"]

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

            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            new_row = pd.DataFrame([[job_id, location, state, date_posted, employer, job_type, day_of_week, description, url, "Dental Post", created_at]], columns=col_names)

            jd_df = pd.concat([jd_df, new_row], ignore_index=True)

        return jd_df, self.website_links

    def execute(self):
        
        self.log_in()
        self.scrape_main_page()
        all_jobs = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        all_links = []
        page_num = 1

        # while page_num < 4:
        #     print("=" * 50)
        #     print("Reading Page ", page_num)

        #     # Scrape main page and each pages
        #     self.scrape_main_page(page_num)
        #     jd_df, website_links = self.scrape_each_page()

        #     # Concatenate all jobs and links
        #     all_jobs = pd.concat([all_jobs, jd_df], ignore_index=True)
        #     all_links += website_links
        #     page_num += 1

        # all_jobs.to_sql('source_dental_post', con=self.engine, if_exists='append', index=False)
        
        return len(all_jobs)


if __name__ == "__main__":
    dp = DentalPost()
    dp_updated_len = dp.execute()