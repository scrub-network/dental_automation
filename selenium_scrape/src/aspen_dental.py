from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
import os
from sqlalchemy import create_engine
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name


class AspenDental:
    def __init__(self, driver):
        self.driver = driver
        engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=engine)
        self.con = engine.connect()
        
    def scrape(self):
        df = pd.DataFrame(columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])
        offset = 0
        while True:
            if offset == 0:
                url = "https://answers-embed.aspendental.com.pagescdn.com/locations.html?query=&referrerPageUrl=&tabOrder=.%2Findex.html%2Cfaqs%2Cservices%2Clocations%2Chealth_articles%2Cblog_posts%2Clinks%2Cproviders&facetFilters=%7B%7D&filters=%7B%7D"
            else:
                url = f"https://answers-embed.aspendental.com.pagescdn.com/locations.html?query=&referrerPageUrl=&tabOrder=.%2Findex.html%2Cfaqs%2Cservices%2Clocations%2Chealth_articles%2Cblog_posts%2Clinks%2Cproviders&facetFilters=%7B%7D&filters=%7B%7D&search-offset={offset}"

            self.driver.get(url)

            try:
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="js-answersVerticalResults"]/section/div/div[1]')))
            except:
                return df
            for i in range(1, 21):
                try:
                    element = self.driver.find_element(By.XPATH, f'//*[@id="js-answersVerticalResults"]/section/div/div[{i}]')
                except:
                    return df
                print(element.text)
                content = element.text.split("\n")
                phone_ind = [i for i, x in enumerate(content) if "(" in x and ")" in x and i > 2][0]
                name = content[1]
                address = content[:phone_ind]
                address = " ".join(address)
                phone = content[phone_ind]
                state = get_full_state_name(address)
                zip_code = address.split(" ")[-1]
                dso = "Aspen Dental"
                scraped_at = pd.Timestamp.now()
                print("Name: ", name)
                print("Address: ", address)
                print("Phone: ", phone)
                print("Zip Code: ", zip_code)
                print("State: ", state)
                print()
                df = pd.concat([df, pd.DataFrame([[name, address, phone, state, zip_code, dso, scraped_at]],
                                                 columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])],
                               ignore_index=True)

            offset += 20

    def execute(self):
        df = self.scrape()
        # Check if scraped data (name + address) already exists in database
        df = df[~df[["name", "address", "dso"]].isin(self.existing_df[["name", "address", "dso"]].to_dict("list")).all(axis=1)]
        # Remove duplicates
        df = df.drop_duplicates(subset=["name", "address", "phone"])
        df["place_id"] = None
        df = df[['place_id', 'name', 'address', 'phone', 'state', 'zip_code', 'dso', 'scraped_at']]
        # Append to existing data
        df.to_sql("dso_practices", con=self.con, schema="dso_scraping", if_exists="append", index=False)
        self.driver.close()

if __name__ == "__main__":
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver")
    AD = AspenDental(driver)
    AD.execute()