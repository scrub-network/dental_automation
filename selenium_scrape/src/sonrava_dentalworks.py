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

class SonravaDentalWorks:
    def __init__(self, driver):
        self.url = "https://topcodental.dentalworks.com/locations/"
        self.engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.engine)
        self.driver = driver

    def scrape(self):
        df = pd.DataFrame(columns=['place_id', 'name', 'address', 'phone',
                                    'state', 'zip_code', 'dso', 'brand',
                                    'latitude','longitude', 'scraped_at'])
        driver.get(self.url)
        self.driver.maximize_window()
        WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="locator-map"]/div[1]/div[3]/div[1]/div[2]')))
        map_item = self.driver.find_element(By.XPATH, '//*[@id="locator-map"]/div[1]/div[3]/div[1]/div[2]')
        self.driver.execute_script("arguments[0].scrollIntoView();", map_item)
        c = 1

        while True:
            # Get total number of results
            wait = WebDriverWait(self.driver, 60)
            try:
                element = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="locator-map"]/div[1]/div[3]/div[1]/div[2]/div/div[3]/div[%d]' % c)))
            except:
                df.to_sql("dso_practices", schema="dso_scraping", con=self.engine, if_exists="append", index=False)
                break
            print("*" * 20, c, "*" * 20)
            # print(element.text)
            self.driver.execute_script("arguments[0].scrollIntoView();", element)
            self.driver.execute_script("arguments[0].click();", element)

            text_element_xpath = '//*[@id="locator-map"]/div[1]/div[3]/div[1]/div[2]/div/div[4]/div/div/div/div[1]'
            wait.until(EC.element_to_be_clickable((By.XPATH, text_element_xpath)))
            pop_up_element = self.driver.find_elements(By.XPATH, text_element_xpath)

            # Get the text of the pop up element
            print(len(pop_up_element))
            txt = pop_up_element[0].text

            if len(pop_up_element) == 0:
                continue

            time.sleep(1)
            try:
                self.driver.find_element(By.XPATH, '/html/body/section[5]/div/div[2]/div/div/div/div[2]/div[2]/div/div[4]/div/div/div/div[1]/button').click()
            except:
                pass

            c += 1
            txt_list = txt.split("\n")
            if len(txt_list) < 4:
                continue

            print(txt)
            print()
            phone_ind = [i for i, s in enumerate(txt_list) if s != 'Directions' and "-" in s and len(s) < 16][0]
            try:
                # See if s.split(" ")[-1] can be converted to an integer
                for i, s in enumerate(txt_list):
                    if len(s.split(" ")[-1]) == 5 and i >= 1:
                        # Check if s is integer
                        try:
                            int(s.split(" ")[-1])
                        except:
                            continue
                        zipcode_ind = i
                        break
                    elif len(s.split(" ")[-1]) == 10 and i >= 1:
                        try:
                            int(s.split("-")[-1])
                        except:
                            continue
                        zipcode_ind = i
                        break
            except:
                zipcode_ind = -1

            practice_name = txt_list[0]
            address = txt_list[:zipcode_ind + 1]
            address = " ".join(address)
            address = address.replace(practice_name + " ", "")
            address_original = address
            # name_address_zipcode = get_practice_info_using_google(address)
            # if "dental" in practice_name.lower() or "family" in practice_name.lower()\
            #     or "dentistry" in practice_name.lower() or "smile" in practice_name.lower()\
            #     or "robinson & prijic" in practice_name.lower() or "grant road" in practice_name.lower():
            #     pass
            # else:
            #     practice_name = name_address_zipcode["Practice Name"]
            # address = name_address_zipcode["Address"]
            # zip_code = name_address_zipcode["Zip Code"]
            # place_id = name_address_zipcode["Place ID"]
            zip_code = address_original.split(" ")[-1]
            if '-' in zip_code:
                zip_code = zip_code.split("-")[0]
            place_id = None
            
            # Change the phone format to "(918) 882-0317" from "918.882.0317" or "918-882-0317"
            if "(" not in txt_list[phone_ind] and txt_list[phone_ind].count("-") == 2:
                # Make '302-369-3200' type to '(302) 369-3200'
                phone = txt_list[phone_ind]
                phone = "(" + phone.split("-")[0] + ") " + phone.split("-")[1] + "-" + phone.split("-")[2]
            elif "(" in txt_list[phone_ind]:
                phone = txt_list[phone_ind]
            else:
                phone = None
            
            state = get_full_state_name(address)
            scraped_at = pd.Timestamp.now()
            print("Place ID: ", place_id)
            print("Practice Name: ", practice_name)
            print("Address: ", address)
            print("Phone: ", phone)
            print("State: ", state)
            print("Zip Code: ", zip_code)
            print()

            df = pd.concat([df, pd.DataFrame([[place_id, practice_name, address, phone, state, zip_code, "Sonrava", "Dental Works", None, None, scraped_at]],
                                             columns=['place_id', 'name', 'address', 'phone',
                                                      'state', 'zip_code', 'dso', 'brand',
                                                      'latitude','longitude', 'scraped_at'])],
                           ignore_index=True)
            df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]
            if c % 100 == 0:
                df["place_id"] = None
                df = df[['place_id', 'name', 'address', 'phone', 'state', 'zip_code', 'dso', 'brand', 'scraped_at']]
                df.to_sql("dso_practices", schema="dso_scraping", con=self.engine, if_exists="append", index=False)
                df = pd.DataFrame(columns=['place_id', 'name', 'address', 'phone',
                                           'state', 'zip_code', 'dso', 'brand',
                                           'latitude','longitude', 'scraped_at'])


    def execute(self):
        df = self.scrape()

if __name__ == "__main__":
    geoAllowed = webdriver.FirefoxOptions()
    geoAllowed.set_preference('geo.prompt.testing', True)
    geoAllowed.set_preference('geo.prompt.testing.allow', True)
    geoAllowed.set_preference('geo.provider.network.url',
        'data:application/json,{"location": {"lat": 0.7128, "lng": 74.0060}, "accuracy": 100.0}')
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver", options=geoAllowed)

    scraper = SonravaDentalWorks(driver)
    scraper.execute()