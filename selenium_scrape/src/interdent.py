from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name
from utils.smile_brands_geomaps_api import get_practice_info_using_google

class Interdent:
    def __init__(self, driver):
        self.url = "https://www.interdent.com/locations/"
        self.engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.engine)
        self.driver = driver

    def scrape(self):
        df = pd.DataFrame(columns=['place_id', 'name', 'address', 'phone',
                                    'state', 'zip_code', 'dso', 'brand',
                                    'latitude','longitude', 'scraped_at'])
        driver.get(self.url)
        self.driver.maximize_window()
        WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/div[2]')))
        map_item = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/div[2]')
        self.driver.execute_script("arguments[0].scrollIntoView();", map_item)
        c = 1

        while True:
            # Get total number of results
            wait = WebDriverWait(self.driver, 10)
            try:
                element = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/div[2]/div/div[3]/div[%d]' % c)))
                time.sleep(1)
            except:
                break
            print("*" * 20, c, "*" * 20)
            # print(element.text)
            self.driver.execute_script("arguments[0].scrollIntoView();", element)
            self.driver.execute_script("arguments[0].click();", element)

            text_element_xpath = '/html/body/div[1]/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/div[2]/div/div[4]/div/div/div/div[1]'
            wait.until(EC.element_to_be_clickable((By.XPATH, text_element_xpath)))
            pop_up_element = self.driver.find_elements(By.XPATH, text_element_xpath)

            # Get the text of the pop up element
            txt = pop_up_element[0].text

            if len(pop_up_element) == 0:
                continue

            time.sleep(1)
            try:
                self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/div[2]/div/div[4]/div/div/div/div[1]/button').click()
            except:
                pass

            c += 1
            txt_list = txt.split("\n")
            if len(txt_list) < 4:
                continue

            print(txt)
            print()
            place_id = None
            address_ind = [i for i, s in enumerate(txt_list) if s == 'View Location Page'][0]
            practice_name = txt_list[0]
            address = txt_list[:address_ind]
            address = " ".join(address)
            address = address.replace(practice_name + " ", "")

            zip_code = address.split(" ")[-1]
            if '-' in zip_code:
                zip_code = zip_code.split("-")[0]
            
            state = get_full_state_name(address)
            scraped_at = pd.Timestamp.now()
            print("Practice Name: ", practice_name)
            print("Address: ", address)
            print("State: ", state)
            print("Zip Code: ", zip_code)
            print()

            df = pd.concat([df, pd.DataFrame([[place_id, practice_name, address, None, state, zip_code, "Interdent", None, None, None, scraped_at]],
                                             columns=['place_id', 'name', 'address', 'phone',
                                                      'state', 'zip_code', 'dso', 'brand',
                                                      'latitude','longitude', 'scraped_at'])],
                           ignore_index=True)
        df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]
        df.reset_index(drop=True, inplace=True)
        df.to_sql("dso_practices", schema="dso_scraping", con=self.engine, if_exists="append", index=False)


    def execute(self):
        df = self.scrape()

if __name__ == "__main__":
    geoAllowed = webdriver.FirefoxOptions()
    geoAllowed.set_preference('geo.prompt.testing', True)
    geoAllowed.set_preference('geo.prompt.testing.allow', True)
    geoAllowed.set_preference('geo.provider.network.url',
        'data:application/json,{"location": {"lat": 0.7128, "lng": 74.0060}, "accuracy": 100.0}')
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver", options=geoAllowed)

    scraper = Interdent(driver)
    scraper.execute()