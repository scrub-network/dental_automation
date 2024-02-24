from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name

class PDSDental:
    def __init__(self, driver):
        self.url = "https://www.pacificdentalservices.com/search-results/#first=10&t=Offices"
        self.engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.engine)
        self.driver = driver

    def scrape(self):
        df = pd.DataFrame(columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])
        _first = 10
        url = f"https://www.pacificdentalservices.com/search-results/#first={_first}&t=Offices"

        while True:
            url = f"https://www.pacificdentalservices.com/search-results/#first={_first}&t=Offices"
            print(url)
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div/div/div/div[7]/div[2]/div[7]/div[2]/div[1]/div[1]/div[2]')))

            if _first == 10:
                time.sleep(10)
            else:
                time.sleep(3)

            # Get total number of results
            ## Results 997-1,000 of 1,045 in 0.12 seconds
            get_total_results = self.driver.find_element(By.XPATH, '//*[@id="pds-search"]/div[7]/div[2]/div[5]')
            total_results = get_total_results.text.split(" ")[-4]
            total_elements = get_total_results.text.split(" ")[1]
            total_result = int(total_results.replace(",", ""))
            element1 = int(total_elements.split("-")[0].replace(",", ""))
            element2 = int(total_elements.split("-")[1].replace(",", ""))
            total_elements = element2 - element1 + 1

            if _first >= total_result + 12:
                return df
            try:
                for i in range(1, total_elements + 1):
                    element = self.driver.find_element(By.XPATH, '/html/body/div/div/div/div/div/div/div[7]/div[2]/div[7]/div[2]/div[%d]' % i)
                    element = element.text.split("\n")
                    name = element[1]
                    if len(name) > 100:
                        continue
                    print("NAME: ", name)
                    address = element[2] + " " + element[3]
                    print("ADDRESS: ", address)
                    phone = element[4]
                    print("PHONE: ", phone)
                    state = get_full_state_name(address)
                    print("STATE: ", state)
                    zip_code = address.split(" ")[-1]
                    print("ZIP CODE: ", zip_code)
                    dso = "Pacific Dental Services"
                    print("DSO: ", dso)
                    scraped_at = pd.Timestamp.now()
                    print("SCRAPE AT: ", scraped_at)
                    print()
                    df = pd.concat([df, pd.DataFrame([[name, address, phone, state, zip_code, dso, scraped_at]],
                                                     columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])],
                                   ignore_index=True)
            except:
                self.driver.get(url)
                time.sleep(5)
                for i in range(1, 13):
                    element = self.driver.find_element(By.XPATH, '/html/body/div/div/div/div/div/div/div[7]/div[2]/div[7]/div[2]/div[%d]' % i)
                    print(element.text)

            _first += 12

    def execute(self):
        df = self.scrape()

        # Check if scraped data (name + address + dso) already exists in database
        df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]

        # Remove duplicates
        df = df.drop_duplicates(subset=["name", "address", "phone"])
        df["place_id"] = None
        df = df[['place_id', 'name', 'address', 'phone', 'state', 'zip_code', 'dso', 'scraped_at']]

        # Append to existing data
        df.to_sql("dso_practices", schema="dso_scraping", con=self.engine, if_exists="append", index=False)

if __name__ == "__main__":
    geoAllowed = webdriver.FirefoxOptions()
    geoAllowed.set_preference('geo.prompt.testing', True)
    geoAllowed.set_preference('geo.prompt.testing.allow', True)
    geoAllowed.set_preference('geo.provider.network.url',
        'data:application/json,{"location": {"lat": 0.7128, "lng": 74.0060}, "accuracy": 100.0}')
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver", options=geoAllowed)

    scraper = PDSDental(driver)
    scraper.execute()