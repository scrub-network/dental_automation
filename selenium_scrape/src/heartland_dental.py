from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import os
from sqlalchemy import create_engine
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.us_states_mapping import get_full_state_name
from utils.geomaps_api import geocode_locations_using_google
import time
import itertools

class HeartlandDental:
    def __init__(self, driver):
        self.driver = driver
        engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.con = engine.connect()
        self.existing_temp_df = pd.read_sql("SELECT * FROM dso_scraping.heartland_dental_address_temp", con=self.con)
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.con)

    def scrape(self):
        df = pd.DataFrame(columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])
        url = "https://heartland.com/affiliate-with-us/"
        self.driver.get(url)
        self.driver.maximize_window()

        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/main/div/section/div[2]/section[5]/div/div[2]/div[2]/div/div[3]/div[2]/div')))
        canvas_element = self.driver.find_element(By.XPATH, '/html/body/div[2]/main/div/section/div[2]/section[5]/div/div[1]/h2')
        self.driver.execute_script("arguments[0].scrollIntoView();", canvas_element)

        # input mapboxgl-ctrl-geocoder--input
        input_element = self.driver.find_element(By.XPATH, '/html/body/div[2]/main/div/section/div[2]/section[5]/div/div[2]/div[2]/div/div[3]/div[2]/div/input')
        self.driver.execute_script("arguments[0].scrollIntoView();", canvas_element)

        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(input_element))
        time.sleep(7)
        input_element.click()

        # Generate list of leading characters more efficiently
        leading_chars = []
        for i in 'abcdefghijklmnopqrstuvwxyz':
            leading_chars.append(i)
            for j in ' abcdefghijklmnopqrstuvwxyz':
                leading_chars.append(i+j)
                for k in ' abcdefghijklmnopqrstuvwxyz':
                    leading_chars.append(i+j+k)

        list_of_suggestions = set()
        suggestion_tags = {}

        for ind, leading_char in enumerate(leading_chars):
            print("*"* 20, ind, "*"*20)
            print("LEADING CHAR: ", leading_char)
            if len(leading_char) == 2 and suggestion_tags.get(leading_char[0], None) == "No results Found":
                print("Parent: ", leading_char[0], " SKIPPED")
                continue
            elif len(leading_char) == 3 and suggestion_tags.get(leading_char[:1], None) == "No results Found":
                print("Parent: ", leading_char[:1], " SKIPPED")
                continue

            # time.sleep(1)
            input_element.send_keys(leading_char)
            time.sleep(1)

            suggestions = self.driver.find_elements(By.XPATH, '//*[@id="map"]/div[3]/div[2]/div/div[1]')
            for suggestion in suggestions:
                suggestion_text = suggestion.text.strip(" \n").split("\n")
                filtered_suggestions = [s for s in suggestion_text
                                        if "United States" not in s
                                        and "Canada" not in s
                                        and "Powered by Mapbox" not in s 
                                        and "Mexico" not in s
                                        and "There was an error reaching the server" not in s]

                if filtered_suggestions == []:
                    pass
                elif filtered_suggestions[0] == 'No results found' and len(leading_char) < 3:
                    suggestion_tags[leading_char] = "No results Found"
                    print("SUGGESTION TAGS: ", suggestion_tags[leading_char])
                    continue
                print("FILTERED SUGGESTIONS: ", filtered_suggestions)

                filtered_suggestions = [s for s in filtered_suggestions
                                        if "No results found" not in s
                                        and "There was an error reaching the server" not in s
                                        and s.strip() != ""]
                list_of_suggestions.update(filtered_suggestions)
            # Remove duplicats for list_of_suggestions
            list_of_suggestions = set(list_of_suggestions)
            print("LIST OF SUGGESTIONS: ", len(list_of_suggestions))
            print()

            input_element.clear()

            # Every 100 iterations, save the list of suggestions to a csv file  
            if ind % 100 == 0 and ind != 0:
                df = pd.DataFrame(list(list_of_suggestions), columns=["address"])
                df = df[~df["address"].isin(self.existing_temp_df["address"].to_list())]
                df = df.drop_duplicates(subset=["address"])
                df.reset_index(drop=True, inplace=True)
                df.to_sql("heartland_dental_address_temp", con=self.con, schema="dso_scraping", if_exists="append", index=False)
                rows_updated = len(df)
                print(f"***** DB UPDATED WITH {rows_updated} NEW PRACTICES *****")

        print("Total Unique Suggestions:", len(list_of_suggestions))
        df = pd.DataFrame(list(list_of_suggestions), columns=["address"])
        df.to_csv("heartland_dental.csv", index=False)

    def fill_data(self):
        sql = "select * from dso_scraping.heartland_dental_address_temp"
        updated_df = pd.read_sql(sql, self.con)
        updated_df = updated_df.drop_duplicates(subset=['address'])
        updated_df.reset_index(drop=True, inplace=True)

        updated_df = updated_df[~updated_df["address"].str.contains("Mexico")]
        updated_df = updated_df[~updated_df["address"].str.contains("Canada")]
        updated_df = updated_df[~updated_df["address"].str.contains("United States")]
        updated_df = updated_df[updated_df['address'] != "No results found"]
        updated_df = updated_df[updated_df['address'] != "There was an error reaching the server"]
        updated_df = updated_df[updated_df['address'].str.strip().astype(bool)]
        updated_df.to_sql("heartland_dental_address_temp", con=self.con, schema="dso_scraping", if_exists="replace", index=False)

        # Get heartland dental from self.existing_df
        hd_exisintg_df = self.existing_df[self.existing_df["dso"] == "Heartland Dental"]
        # Add name + address for hd_exisintg_df
        hd_exisintg_df["name_address"] = hd_exisintg_df["name"] + "," + hd_exisintg_df["address"]

        for ind, row in hd_exisintg_df.iterrows():
            if row["name_address"] in updated_df["address"].values:
                updated_df = updated_df[updated_df["address"] != row["name_address"]]
        updated_df.reset_index(drop=True, inplace=True)

        df = pd.DataFrame(columns=["name", "address", "phone", "state", "zip_code", "dso", "scraped_at"])
        df = geocode_locations_using_google(updated_df)
        df["name"] = updated_df["address"].apply(lambda x: x.split(",")[0])
        df["address"] = updated_df["address"].apply(lambda x: x.split(",",1)[1])
        df["state"] = updated_df["address"].apply(lambda x: get_full_state_name(x))
        df["dso"] = "Heartland Dental"
        df["scraped_at"] = pd.Timestamp.now()
        df["place_id"] = None
        df = df[['place_id', 'name', 'address', 'phone', 'state', 'zip_code', 'dso', 'scraped_at']]
        df.to_sql("dso_practices", con=self.con, schema="dso_scraping", if_exists="append", index=False)

    def execute(self):
        self.fill_data()
        self.scrape()
        self.driver.close()
        self.fill_data()

if __name__ == "__main__":
    # Initialize Firefox WebDriver
    driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver")

    # Create an instance of HeartlandDental and execute the scraping
    HD = HeartlandDental(driver)
    HD.execute()
