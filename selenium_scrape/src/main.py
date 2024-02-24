import pandas as pd
from selenium import webdriver
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import googlemaps
import re
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.generate_place_ids import get_place_details
from aspen_dental import AspenDental
from heartland_dental import HeartlandDental
from pacific_dental_services import PDSDental
from smile_brands import SmileBrands
from sonrava import Sonrava


class ScrapeAllDSOs:
    def __init__(self):
        self.engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
        self.driver = webdriver.Firefox(executable_path="selenium_scrape/src/geckodriver")

    def run_all_automations(self):
        AspenDental(self.driver).execute()
        HeartlandDental(self.driver).execute()
        PDSDental(self.driver).execute()
        SmileBrands(self.driver).execute()
        Sonrava(self.driver).execute()

    def fill_missing_place_ids(self):
        self.existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=self.engine)
        missing_place_ids_df = self.existing_df[(self.existing_df["place_id"].isnull()) | (self.existing_df["latitude"].isnull()) | (self.existing_df["longitude"].isnull())]
        for i, row in missing_place_ids_df.iterrows():
            print("*" * 20, i, "*" * 20)
            name = row["name"]
            address = row["address"]
            zipcode = row["zip_code"]
            place_details = get_place_details(name, address, zipcode)
            self.existing_df.loc[i, "place_id"] = place_details["Place ID"]
            print(place_details["Place ID"])
            self.existing_df.loc[i, "latitude"] = place_details["Latitude"]
            self.existing_df.loc[i, "longitude"] = place_details["Longitude"]
        self.existing_df.to_sql("dso_practices", schema="dso_scraping", con=self.engine, if_exists="append", index=False)

scrape_all_dsos = ScrapeAllDSOs()
scrape_all_dsos.fill_missing_place_ids()




