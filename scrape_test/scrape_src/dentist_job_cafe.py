import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import requests
import time
import random
import mechanize
import urllib3
import http.cookiejar
import re
import uuid
from utils.us_states_mapping import get_full_state_name
from datetime import datetime
from sqlalchemy import create_engine, text

class DentalJobCafe:
    def __init__(self):
        s = Service(ChromeDriverManager().install())
        # self.driver = webdriver.Chrome(service=s)
        # self.actions = ActionChains(self.driver)
        self.engine = create_engine('postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs')
        self.existing_df = pd.read_sql_query('select * from source_dentist_job_cafe',con=self.engine)
        self.url = "https://www.dentistjobcafe.com/jobs/search?criteria=Dentist&posted_period=all&results_per_page=50&sort_order=date_posted&page="

    def log_in(self):
        username = "seanhyang1@gmail.com"
        password = "fifaexecutivedrive%1"
        login_url = "https://www.dentistjobcafe.com/login"

        cj = http.cookiejar.CookieJar()
        self.br = mechanize.Browser()
        self.br.set_cookiejar(cj)
        self.br.open(login_url)

        self.br.select_form(nr=0)
        self.br.form['_username'] = username
        self.br.form['_password'] = password
        self.br.submit()

    def scrape_main_page(self, page_num=1):
        self.br.open(self.url + str(page_num))
        html_content = self.br.response().read()
        soup = BeautifulSoup(html_content, "lxml")

        # Extract all href attributes
        a_tags = soup.find_all('a')

        # Extract href attributes from each <a> tag
        self.website_links = ['https://www.dentistjobcafe.com' + str(tag['href']) 
                 for tag in a_tags if tag.has_attr('href') and '/job/dentist/' in tag['href']]
        self.website_links = list(set(self.website_links))

        max_page = soup.find('ul', class_="pagination")
        max_page = max_page.find_all('li')[-2].text.strip()

        return max_page

    def scrape_each_page(self):

        def extract_company_and_role_name(elements):
            # Filter out empty or whitespace-only elements
            cleaned_elements = [e.strip() for e in elements if e.strip() != '']

            # Remove any empty space or ': '
            if ': ' in cleaned_elements:
                cleaned_elements = [e.replace(': ', '') for e in cleaned_elements]

            return cleaned_elements[-1]

        batch_df = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        existing_job_ids = list(self.existing_df["job_id"].values)

        for url in self.website_links:
            # create a uuid given the url
            job_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
            if job_id in existing_job_ids:
                print("DENTIST JOB CAFE: Skipping ", url)
                continue

            print("DENTIST JOB CAFE: Reading ", url)
            self.br.open(url)
            html_content = self.br.response().read()
            soup = BeautifulSoup(html_content, "lxml")

            # Get company name
            div = soup.find('div', class_="col-sm-7 job-view-header-left")

            # Find all texts
            div_text = div.find_all(text=True)
            company = extract_company_and_role_name(div_text)

            # Find text that has <label class="margin-top-xs">Specialty:</label> that has parent p class col-xs-12 border-radius-4 label-border
            specialty_label = soup.find('label', class_="margin-top-xs", text="Specialty:")
            position = specialty_label.find_parent('p').text.strip()
            position = extract_company_and_role_name(position.split('\n'))

            location_label = soup.find('label', class_="margin-top-xs", text="Location:")
            location = location_label.find_parent('p').text.strip()
            location = extract_company_and_role_name(location.split('\n'))

            job_type_label = soup.find('label', class_="margin-top-xs", text="Position type:")
            job_type = job_type_label.find_parent('p').text.strip()
            job_type = extract_company_and_role_name(job_type.split('\n'))

            date_posted = soup.find('span', class_="btn btn-link text-muted-hard bg-muted-hard cursor-default box-shadow-none").text.strip().replace("Last Updated: ", "")
            date_posted = datetime.strptime(date_posted, "%m/%d/%y").strftime("%Y-%m-%d")

            job_description_p = soup.find('p', class_="text-justify job-description margin-bottom-xl")
            job_description = job_description_p.find_parent('div').text.strip()

            state = get_full_state_name(location)

            # Append to dataframe
            new_row = {"job_id": job_id, "location": location, "state": state, "date_posted": date_posted, "employer": company,
                       "job_type": job_type, "days_of_week": "", "description": job_description, "post_link": url, "source": "Dentist Job Cafe", "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            
            # Check if the job_id already exists in the database
            batch_df = pd.concat([batch_df, pd.DataFrame([new_row])])
        
        return batch_df

    def execute(self):
        self.log_in()
        all_jobs = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        max_page = 10
        page_num = 1

        while int(max_page) >= page_num:
            print("=" * 50)
            print("Reading Page ", page_num)
            max_page = self.scrape_main_page(page_num)
            batch_df = self.scrape_each_page()
            all_jobs = pd.concat([all_jobs, batch_df])
            page_num += 1

        all_jobs.to_sql("source_dentist_job_cafe", con=self.engine, if_exists="append", index=False)


if __name__ == "__main__":
    DP = DentalPost()
    DP.execute()
