import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import uuid
import openai
import re
from utils.us_states_mapping import get_full_state_name
from utils.dso_mapping import generate_dso_mapping
from sqlalchemy import create_engine
from airflow.models import Variable

class AdaCareerCenter:
    def __init__(self):
        self.url = "https://careercenter.ada.org/searchjobs/?keywords=dentist&countrycode=US&Page="
        db_uri = "postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs"
        self.engine = create_engine(db_uri)
        self.existing_df = pd.read_sql_query('select * from source_ada_careercenter',con=self.engine)
        self.dso_mapping = pd.read_sql_query('select * from dental_mapping.dso_mapping', con=self.engine)

    def find_last_page(self):
        pg_start_from = 181
        div_blocks = []
        while div_blocks == []:
            pg_start_from -= 1
            url = "https://careercenter.ada.org/searchjobs/?keywords=dentist&countrycode=US&Page=" + f"{pg_start_from}"
            html_content = requests.get(url).text
            soup = BeautifulSoup(html_content, "lxml")
            div_blocks = soup.find_all('li', class_="lister__item")
        return pg_start_from

    def scrape_main_page(self, page_num=1):
        self.url = "https://careercenter.ada.org/searchjobs/?keywords=dentist&countrycode=US&Page=" + f"{page_num}"
        print("Updated URL ", self.url)
        html_content = requests.get(self.url).text
        soup = BeautifulSoup(html_content, "lxml")

        div_blocks = soup.find_all('li', class_="lister__item")

        self.date_posted = []
        self.website_links = []
        for block in div_blocks:
            if "ad__notice" in str(block):
                continue
            try:
                ago = block.find('li', class_="job-actions__action pipe").text.strip()
                if "ago" not in ago:
                    self.date_posted.append(None)
                else:
                    post_date = datetime.today() - timedelta(days=int(ago.split(" ")[0]))
                    self.date_posted.append(post_date.strftime("%Y-%m-%d"))
                self.website_links.append("https://careercenter.ada.org" + re.sub(r'\s+', '', block.find('a', class_='js-clickable-area-link')["href"]))
            except:
                self.date_posted.append(None)
                self.website_links.append("https://careercenter.ada.org" + re.sub(r'\s+', '', block.find('a', class_='js-clickable-area-link')["href"]))


    def scrape_each_page(self):
        # Column names
        col_names = ["job_id", "job_title", "location", "state", "date_posted", "employer", "employer_type", "job_type", "days_of_week", "description", "post_link", "source", "created_at"]

        # Create an empty dataframe
        jd_df = pd.DataFrame(columns=col_names)
        existing_job_ids = list(self.existing_df['job_id'].values)

        for ind, url in enumerate(self.website_links):

            # Get rid of / at the end of the url
            url = url.rstrip('/')

            # create a uuid given the url
            job_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
            if job_id in existing_job_ids:
                print("ADA: Skipping ", url)
                continue

            print("ADA: Reading ", url)
            html_content = requests.get(url).text
            soup = BeautifulSoup(html_content, "lxml")

            # Below inclues:
            keys = soup.find_all('dt', class_="mds-list__key")
            keys = [key.text.strip() for key in keys]
            values = soup.find_all('dd', class_="mds-list__value")
            values = [value.text.strip() for value in values]
            values = [value.replace("\n", "").replace("  ", "") for value in values]
            job_details = dict(zip(keys, values))

            # lcoation
            try:
                location = job_details['Location']
            except:
                location = None

            # state
            try:
                state = get_full_state_name(location)
            except:
                state = None

            # date posted
            try:
                date_posted = self.date_posted[ind]
            except:
                date_posted = None

            # employer
            try:
                employer = job_details['Employer']
            except:
                employer = None
            
            # employer type
            if employer != None:
                employer_type = generate_dso_mapping(employer, self.dso_mapping)
            else:
                employer_type = None

            # job type
            try:
                job_type = job_details['Job Type']
            except:
                job_type = None

            # days of week
            day_of_week = None

            # job description
            try:
                description = soup.find_all('div', class_="mds-tabs__panel__content")[0].text.strip()
            except IndexError:
                description = "Undefined"

            # Job Title
            try:
                job_title = job_details["Position"]
            except:
                job_title = None

            # created_at
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # If there are too many None values, skip
            new_row = pd.DataFrame([[job_id, job_title, location, state, date_posted, employer, employer_type, job_type, day_of_week, description, url, "ADA", created_at]], columns=col_names)

            if new_row.isnull().sum().sum() > 3:
                print("ADA: Skipping ", url)
                continue
            jd_df = pd.concat([jd_df, new_row], ignore_index=True)

        return jd_df, self.website_links

    def execute(self, last_page):
        all_jobs = pd.DataFrame(columns=["job_id", "location", "state", "date_posted", "employer", "employer_type", "job_type", "days_of_week", "description", "post_link", "source", "created_at"])
        all_links = []
        page_num = 1

        while page_num <= last_page:
            print("=" * 50)
            print("Reading Page ", page_num)

            # Scrape main page and each pages
            self.scrape_main_page(page_num)

            jd_df, website_links = self.scrape_each_page()

            # Concatenate all jobs and links
            jd_df.to_sql('source_ada_careercenter', con=self.engine, if_exists='append', index=False)
            all_jobs = pd.concat([all_jobs, jd_df], ignore_index=True)
            all_links += website_links

            # Progress bar
            page_num += 1

if __name__ == "__main__":
    ada = AdaCareerCenter()
    last_page = ada.find_last_page()
    ada.execute(last_page)