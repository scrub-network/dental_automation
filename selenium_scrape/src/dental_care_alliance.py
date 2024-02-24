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

        websites = {"Main Street": "https://www.mainstreetsmiles.com/dentist-office",
                    "Town Care Dental": "https://www.towncaredental.com/dentist-office",
                    "Advanced Dental Care": "https://www.adc-fl.com/dentist-office",
                    "Dental Specialty Centers": "https://www.dentalspecialtyflorida.com/dentist-office",
                    "Dental Solutions": "https://dentalsolutionscreatingsmiles.com/dentist-office",
                    "Baystate Dental PC": "https://baystate-dental.com/dentist-office",
                    "Dental Associates": "https://smilesincluded.com/dentist-office",
                    "Dental One Associates": "https://www.dentalone-md.com/dentist-office",
                    "Konikoff Dentistry": "https://konikoffdental.com/dentist-office",
                    "Dental Associates of NV": "https://dentalassociatesnova.com/dentist-office",
                    "Gentle Dental": "https://www.gentledental-pa.com/dentist-office",
                    "Imagix Dental": "https://imagixdental.com/dentist-office",
                    "The Dental Center": "https://www.dentalcenter-in.com/dentist-office",
                    "Florida Dental Center": "https://www.floridadentalcenters.com/dentist-office",
                    "Gentle Dental": "https://www.gentledental-mi.com/dentist-office",
                    "Long Island Dental Specialty": "https://www.longislanddentalspecialty.com/dentist-office",
                    "Contemporary Dentistry": "https://www.contemporarydental.com/dentist-office",
                    "Dental One Associates": "https://www.dentalone-ga.com/dentist-office",
                    "Dental One Associates": "https://www.dentalone-va.com/dentist-office",
                    "Family Dental Group": "https://thefamilydentalgroup.com/dentist-office",
                    "Garden State Dental": "https://gardenstatedental.com/dentist-office",
                    "Premier Dental": "https://www.premierdentalconnecticut.com/dentist-office",
                    "Orthodontics": "https://www.southtexasorthodontics.com/dentist-office",
                    "Konikoff Dentistry": "https://www.konikoffkids.com/dentist-office",
                    "Maple Shade Dental Group": "https://www.mapleshadecenter.com/dentist-office",
                    "NVOMSA": "https://nvomsa.com/dentist-office",
                    "Arkansas Maxillofacial": "https://www.arkansasmaxoralsurgery.com/dentist-office",
                    "Colorado Springs": "https://csoafs.com/dentist-office",
                    "Dental Specialty Center": "https://www.dentalspecialtyvirginia.com/dentist-office",
                    "Ridgewood Oral Surgery": "https://www.ridgewoodoralsurgery.com/dentist-office",
                    "Taheri Dental Group": "https://taheridentalgroup.com/dentist-office",
                    "Tioga Dental & Orthodontics": "https://www.tiogadental.com/dentist-office",
                    "Wadas Dental Center": "https://www.wadasdental.com/dentist-office"}

        for brand_name, url in websites.items():
            
            # Get the list of abbreviation of states
            self.driver.get(url)
            WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="app"]/main/section[2]/div/div/div/div/div[2]/div[1]/ul/li[1]')))

            c = 1
            while True:
                try:
                    element = self.driver.find_element(By.XPATH, '//*[@id="app"]/main/section[2]/div/div/div/div/div[2]/div[1]/ul/li[%d]' % c)
                except:
                    break
                element_text = element.text
                print("*" * 20, c, "*" * 20)
                print(element_text)
                print()
                # print(element_text)
                # print()
                element_text_lst = element_text.split("\n")

                # practice name
                practice_name = element_text_lst[0].title()

                # address
                website_ind = [i for i in element_text_lst if "Website" in i]
                address = element_text_lst[:element_text_lst.index(website_ind[0])]
                address = " ".join(address)
                address = address.replace(practice_name + " ", "")
                state = get_full_state_name(address)
                phone = element_text_lst[element_text_lst.index(website_ind[0]) + 1]
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
                    "dso": ["Dental Care Alliance"],
                    "brand": [brand_name],
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