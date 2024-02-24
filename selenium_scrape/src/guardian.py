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


class Gaurdian:
    def __init__(self, driver):
        self.url = "https://www.guardiandentistry.com/our-network"
        engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.driver = driver
        self.con = engine.connect()
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.con)

    def scrape_western_dental(self):

        self.driver.maximize_window()
        df = pd.DataFrame(columns=['place_id', 'name', 'address', 'phone',
                                    'state', 'zip_code', 'dso', 'brand',
                                    'latitude','longitude', 'scraped_at'])

        self.driver.get(self.url)

        # Get total results
        wait = WebDriverWait(self.driver, 60)
        element = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div/div/div[2]/div/div/div/div[3]/div[1]/div[2]')))
        self.driver.execute_script("arguments[0].scrollIntoView();", element)

        c = 1
        while True:
            try:
                element = self.driver.find_element(By.XPATH, '//*[@id="map"]/div/div[3]/div[1]/div[2]/div/div[3]/div[%d]' % c)
            except:
                break
            self.driver.execute_script("arguments[0].scrollIntoView();", element)
            self.driver.execute_script("arguments[0].click();", element)

            text_element_xpath = '//*[@id="map"]/div/div[3]/div[1]/div[2]/div/div[4]/div/div/div/div[1]'
            wait.until(EC.element_to_be_clickable((By.XPATH, text_element_xpath)))
            pop_up_element = self.driver.find_elements(By.XPATH, text_element_xpath)

            element_text = pop_up_element[0].text

            if len(pop_up_element) == 0:
                continue

            time.sleep(1)
            try:
                self.driver.find_element(By.XPATH, '/html/body/main/section[3]/div/div/div[2]/div/div/div/div[3]/div[1]/div[2]/div/div[4]/div/div/div/div[1]/button').click()
            except:
                pass

            print("*" * 20, c, "*" * 20)
            print(element_text)
            print()
            element_text_lst = element_text.split("\n")

            # practice name
            practice_name = element_text_lst[0]

            # address
            get_directions_ind = [i for i in element_text_lst if "Visit Website" in i]
            check_phone_visit = [i for i in element_text_lst if i.count("-") == 2 or "Visit Website" in i]
            if len(check_phone_visit) == 0:
                address_ind = len(element_text_lst)
            elif len(get_directions_ind) == 0:
                address_ind = len(element_text_lst) - 1
            else:
                address_ind = element_text_lst.index(get_directions_ind[0]) - 1
            address = element_text_lst[:address_ind]
            address = " ".join(address)
            address = address.replace(practice_name + " ", "")
            state = get_full_state_name(address)
            if len(check_phone_visit) == 0:
                phone = None
            else:
                phone = element_text_lst[address_ind]

            if phone is None:
                pass
            elif "(" not in phone and phone.count("-") == 2:
                # Make '302-369-3200' type to '(302) 369-3200'
                phone = phone
                phone = "(" + phone.split("-")[0] + ") " + phone.split("-")[1] + "-" + phone.split("-")[2]
            elif "(" in phone:
                phone = phone
            else:
                phone = None
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
                "dso": ["Guardian"],
                "brand": [None],
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

    scraper = Gaurdian(driver)
    scraper.execute()