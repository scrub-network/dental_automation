from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import requests
import re
import us
import openai
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name
from utils.smile_brands_geomaps_api import get_practice_info_using_google


class AffordableCareADI:
    def __init__(self, driver):
        self.url = "https://locations.dentalservice.net/?q=United%20States"
        engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.driver = driver
        self.con = engine.connect()
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.con)

    def scrape_western_dental(self):

        self.driver.maximize_window()

        df = pd.DataFrame(columns=["place_id", "name", "address", "phone",
                                   "state", "zip_code", "dso", "brand",
                                   "latitude", "longitude", "scraped_at"])

        # Get the list of abbreviation of states
        self.driver.get(self.url)
        WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div/div/div[1]/div[3]/div/div[1]')))

        c = 1
        while True:
            try:
                element = self.driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div[1]/div[3]/div/div[%d]' % c)
            except:
                break
            element_text = element.text
            print("*" * 20, c, "*" * 20)
            # print(element_text)
            # print()
            element_text_lst = element_text.split("\n")
            
            # practice name
            practice_name = element_text_lst[0].title()
            
            # address
            get_directions_ind = [i for i in element_text_lst if "Get Directions" in i]
            address = element_text_lst[:element_text_lst.index(get_directions_ind[0]) - 2]
            address = " ".join(address)
            address = address.replace(practice_name + " ", "")
            state = get_full_state_name(address)
            phone = element_text_lst[element_text_lst.index(get_directions_ind[0]) - 1]
            zip_code = address.split(" ")[-1]
            print("Practice Name: ", practice_name)
            print("Address: ", address)
            print("State: ", state)
            print("Zip Code: ", zip_code)
            print("Phone: ", phone)
            print()

            c += 1

            row_df = pd.DataFrame({
                "place_id": [None],
                "name": [practice_name],
                "address": [address],
                "phone": [phone],
                "state": [state],
                "zip_code": [zip_code],
                "dso": ["Affordable Care"],
                "brand": ["DDS"],
                "latitude": [None],
                "longitude": [None],
                "scraped_at": [pd.Timestamp.now()]
            })
            df = pd.concat([df, row_df], ignore_index=True)
        
        # make sure the df address, dso are not in the existing df
        df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]
        df = df.reset_index(drop=True)
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

    scraper = AffordableCareADI(driver)
    scraper.execute()