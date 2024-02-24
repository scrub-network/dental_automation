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
import uuid
import random
import mechanize
import urllib3
import http.cookiejar
import re
from utils.us_states_mapping import get_full_state_name
from utils.string_jobtype import extract_jobtype
from utils.posted_dates import get_posted_dates
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse

class ScrapeIndeed:
    def __init__(self):
        self.scrapfly = ScrapflyClient(key='scp-live-9802abf45a2145e9829d1c21d183730e')
        self.url = "https://www.indeed.com/jobs?q=dentist&l=&sort=date&vjk=a157856c74b527d0"
        self.start_page_num = 0

    def scrape_main_page(self):
        def extract_links(html_content):
            # Extract href attributes            
            return ['https://www.indeed.com' + link['href'] 
                     for link in BeautifulSoup(html_content, 'html.parser').find_all('a') 
                     if link.has_attr('href')]

        if self.start_page_num > 0:
            self.url = self.url + '&start=' + str(self.start_page_num)

        with self.scrapfly as scraper:
            response:ScrapeApiResponse = scraper.scrape(ScrapeConfig(url=str(self.url), country='fr', retry=True, asp=True))
            result = response.scrape_result
            print("CONNECTED - Page " + str(self.start_page_num / 10))
            self.start_page_num += 10

        content = result['content']
        soup = BeautifulSoup(content, 'html.parser')
        main_divs = soup.find_all('li', class_='css-5lfssm eu4oa1w0')
        dates_spans = soup.find_all('span', class_='date')

        self.jp_page_links = [extract_links(str(div))[0] for div in main_divs if extract_links(str(div)) != []]
        self.jp_page_dates = [span.text for span in dates_spans]
        self.jp_page_dates = [get_posted_dates(date) for date in self.jp_page_dates]

    def scrape_each_page(self, link, posted_date):

        with self.scrapfly as scraper:
            response:ScrapeApiResponse = scraper.scrape(ScrapeConfig(url=str(link), country='fr', retry=True, asp=True))
            page_result = response.scrape_result
            page_content = page_result['content']
            soup = BeautifulSoup(page_content, 'html.parser')
            print("CONNECTED - " + link)

        # Get Job ID
        job_id = str(uuid.uuid3(uuid.NAMESPACE_URL, link))

        # Get Title
        title_h1 = soup.find_all('h1', class_='jobsearch-JobInfoHeader-title')
        try:
            title = title_h1[0].text
        except IndexError:
            title = ""

        # Get Company Name
        employer_div = soup.find_all('div', {'data-testid': 'inlineHeader-companyName'})
        employer = employer_div[0].text

        # Get Location
        location_div = soup.find_all('div', {'data-testid': 'inlineHeader-companyLocation'})
        location = location_div[0].text

        # Get State
        state = get_full_state_name(location)

        # Get Job Description
        job_description = soup.find_all('div', id='jobDescriptionText')
        job_description_cleaned = '\n'.join([element.get_text() for element in soup.find_all(['h5', 'p']) if element.get_text().strip()])

        # Get salary and jobtype
        salary_jobtype_div = soup.find_all('div', id='salaryInfoAndJobType')
        try:
            salary_jobtype = salary_jobtype_div[0].text
            job_type = extract_jobtype(salary_jobtype)
        except IndexError:
            salary_jobtype = ""
            job_type = ""
        
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        new_row_df = pd.DataFrame([[job_id, employer, state, location, salary_jobtype, job_type, "", job_description_cleaned, posted_date, link, "Indeed", created_at]], 
                                  columns=["job_id", "employer", "state", "location", "compensation", "job_type", "days_of_week", "description", "date_posted", "post_link", "source"])
        return new_row_df

    def execute(self):
        indeed_df = pd.DataFrame(columns=["job_id", "employer", "state", "location","compensation", "job_type", "days_of_week", "description", "date_posted", "post_link", "source"])
        while True:
            self.scrape_main_page()
            for ind, link in enumerate(self.jp_page_links):
                new_row = self.scrape_each_page(link, self.jp_page_dates[ind])
                indeed_df = pd.concat([indeed_df, new_row])
            if self.start_page_num % 100 == 0:
                indeed_df.to_csv(f"scrape_test/data/indeed_{self.start_page_num}.csv", index=False)
            if new_row["job_id"].iloc[0] in list(indeed_df["job_id"].values):
                break
        indeed_df.to_csv(f"scrape_test/data/indeed_jp_data.csv", index=False)


if __name__ == "__main__":
    scrape = ScrapeIndeed()
    scrape.execute()