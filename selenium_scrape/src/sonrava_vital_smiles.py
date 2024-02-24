import pandas as pd
from sqlalchemy import create_engine, text

url = "https://www.vitalsmiles.com/locations/"
engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
con = engine.connect()
existing_df = pd.read_sql("SELECT * FROM dso_scraping.dso_practices", con=con)

# Jan 27, 2024
now_date = pd.Timestamp.now()
new_dic = {"place_id": [None, None, None, None, None],
           "name": ["Mobile – Pleasant Valley Family Center",
                    "Center Point",
                    "Center Point Orthodontic Clinic",
                    "Midfield",
                    "Huntsville Family Center"], 
           "address": ["2727 Pleasant Valley Rd. Mobile, AL 36606",
                       "2302 Center Point Parkway Birmingham, AL 35215",
                       "2525 Center Point Parkway Center Point, AL 35215",
                       "111 B.Y. Williams Sr. Drive Midfield, AL 35228",
                       "3700-F Blue Spring Road Huntsville, AL 35810"], 
           "phone": ["(251) 473-5705", "(205) 853-9170", "(205) 854-8093", "(205) 923-3172", "(256) 852-9994"],
           "state": ["Alabama", "Alabama", "Alabama", "Alabama", "Alabama"],
           "zip_code": ["36606", "35215", "35215", "35228", "35810"],
           "dso": ["Sonrava", "Sonrava", "Sonrava", "Sonrava", "Sonrava"],
           "brand": ["Vital Smiles", "Vital Smiles", "Vital Smiles", "Vital Smiles", "Vital Smiles"],
           "latitude": [None, None, None, None, None],
           "longitude": [None, None, None, None, None],
           "scraped_at": [now_date, now_date, now_date, now_date, now_date]}

new_df = pd.DataFrame(new_dic)
new_df.to_sql("dso_practices", schema="dso_scraping", con=con, if_exists="append", index=False)