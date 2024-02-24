from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import requests
import re
import openai
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name
from utils.smile_brands_geomaps_api import get_practice_info_using_google


class SonravaBrident:
    def __init__(self, driver):
        self.url = "https://www.sonrava.com/locations/"
        engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.driver = driver
        self.con = engine.connect()
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.con)

    def scrape_western_dental(self):

        self.driver.maximize_window()
        df = pd.DataFrame(columns=['place_id', 'name', 'address', 'phone',
                                    'state', 'zip_code', 'dso', 'brand',
                                    'latitude','longitude', 'scraped_at'])

        for state in ["arizona", "colorado", "delaware", "maryland", "new jersey", "new mexico", "nevada", "texas"]:
            url = "https://www.brident.com/en-us/find-a-location/%s" % state

            self.driver.get(url)

            # Get total results
            WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[6]/main/div[2]/section[2]/div/div/div[2]/div[1]/div/div/span')))
            total_results = self.driver.find_element(By.XPATH, '/html/body/form/div[6]/main/div[2]/section[2]/div/div/div[2]/div[1]/div/div/span').text
            total_results = int(total_results.split(" ")[0])

            for i in range(1, total_results + 1):
                print("*" * 20, i, "/", total_results, "*" * 20)
                # Get the element
                element_wait = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[6]/main/div[2]/section[2]/div/div/div[2]/div[2]/div/div[1]/div/div[%d]' % i)))
                self.driver.execute_script("arguments[0].scrollIntoView();", element_wait)
                time.sleep(1)
                element = self.driver.find_element(By.XPATH, '/html/body/form/div[6]/main/div[2]/section[2]/div/div/div[2]/div[2]/div/div[1]/div/div[%d]' % i)
                element_text = element.text
                element_text_lst = element_text.split("\n")

                practice_name = element_text_lst[0].title()
                # (559) 584-9200 type
                print(element_text)
                try:
                    phone = [i for i in element_text_lst if re.search(r"\(\d{3}\) \d{3}-\d{4}", i)][0]
                except:
                    phone = None
                # Address is the next line after "MAKE AN APPOINTMENT"
                address_1 = element_text_lst[element_text_lst.index("LOCATION DETAILS") + 1]
                address_2 = element_text_lst[element_text_lst.index("LOCATION DETAILS") + 2]
                address = address_1 + " " + address_2
                # Change from all caps to title case

                state = get_full_state_name(address)
                address = address.title()
                # address_results = get_practice_info_using_google(address)
                # address = address_results["Address"]
                # zip_code = address_results["Zip Code"]
                scraped_at = pd.Timestamp.now()

                print('practice_name:', practice_name, '\n', 'phone:', phone, '\n',
                      'address:', address, '\n', 'state:', state, '\n')#, "zip code: ", zip_code, '\n')
                
                row_df = pd.DataFrame({
                    "name": [practice_name],
                    "address": [address],
                    "phone": [phone],
                    "state": [state],
                    "zip_code": None,
                    "dso": ["Sonrava"],
                    "brand": ["Brident"],
                    "latitude": None,
                    "longitude": None,
                    "scraped_at": [scraped_at]
                })
                df = pd.concat([df, row_df], ignore_index=True)

        # make sure the df address, dso are not in the existing df
        df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]
        df = df.reset_index(drop=True)
        df["place_id"] = None
        df = df[['place_id', 'name', 'address', 'phone',
                 'state', 'zip_code', 'dso', 'brand',
                 'latitude','longitude', 'scraped_at']]
        df.to_sql("dso_practices", schema="dso_scraping", con=self.con, if_exists="append", index=False)

    def execute(self):
        self.scrape_western_dental()

if __name__ == "__main__":
    geoAllowed = webdriver.FirefoxOptions()
    geoAllowed.set_preference('geo.prompt.testing', True)
    geoAllowed.set_preference('geo.prompt.testing.allow', True)
    geoAllowed.set_preference('geo.provider.network.url',
        'data:application/json,{"location": {"lat": 0.7128, "lng": 74.0060}, "accuracy": 100.0}')
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver", options=geoAllowed)

    scraper = SonravaBrident(driver)
    scraper.execute()