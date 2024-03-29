import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

def get_brand_names():
    db_uri = st.secrets["db_uri"]
    engine = create_engine(db_uri)
    dso_practices_sql = text("SELECT * FROM dso_scraping.dso_practices")
    existing_df = pd.read_sql(dso_practices_sql, engine)

    def manual_adds():
        brand_name_dic = {"brand": ['All Smiles', 'Acworth Center for Family Dentistry',
                            'Advanced Dentistry of Athens', 'Affiliated Dental Specialists', 'Aggie Dental Center', 'Alcovy Family & Cosmetic Dentistry',
                            'Altima Dental', 'Associated Dental Specialists of Long Grove', 'AuraSmile Dental', 'BB Braces Braces',
                            'BE Orthodontics', 'Bright Downtown Dental Arts', 'BWS Oral and Maxillofacial Surgery', 'Cardinal Family Dental',
                            'Corner Dental', 'Complete Dental Care', 'Convenient Family Dental', 'Covington Center for Family Dentistry',
                            'Crabapple Family Dentistry', 'Dental Alternatives', 'Dental Associates of Warner Robins', 'Dental Care of Columbus',
                            'Dental Care of Indiana', 'Dental Care of Michigan', 'Dental Care of Texas', 'Dental Express', 'Dentists on Main',
                            'East Texas Dental Group', 'Eaves Family Dental', 'Endodontics Associates of Maryland', 'Family Gentle Dentists',
                            'Flat Rock Dental Center', 'Grove Dental Associates', 'Hiram Center for Family Dentistry', 'Hopewell Dental',
                            'Innovate Dental', 'Kaye Dentistry', 'Kids First Pediatric Dentistry', 'Life Long Dental Care',
                            'Maplewood Family Dental', 'McDonough Center for Family Dentistry', 'MMS Endodontics Specialists',
                            'MMS Dental Implants & Periodontics', 'Monument Periodontics', 'New Image Dentistry', 'Oakcrest Family Dental',
                            'Oral and Maxillofacial Surgery Associates of WNY', 'Parkersburg Endodontics', 'Parkway Dental Care',
                            'Park56 Dental', 'Precision Specialty Group', 'Refresh Dental', 'Scarsdale Oral Surgery',
                            'Smiles4Kids Manhattan Pediatric Dental', 'Snodgrass-King Dental', 'Sorrento Dental Care', 'Southeast Orthodontics',
                            'St. Petersburg Dental Center', 'Sterling Dental', 'St. Petersburg Dental Center', 'Stonecreek Dental Care',
                            'Stonewalk Center for Family Dentistry', 'Stoneybrook Dental', 'Suwanee Center for Family Dentistry',
                            'Tech Dentistry Center for Family Dentistry', 'Trenton Family Dental', 'Westermeier Martin Dental Care',
                            'Whittaker Family Dental', 'Willow Orthodontics', 'Winning Smiles', 'Woodland Lake Family Dental',
                            'Yonkers Oral Surgery']}
        brand_name_dic["dso"] = ["North American Dental Group"] * len(brand_name_dic["brand"])
        brand_name_dic["name"] = [None] * len(brand_name_dic["brand"])
        brand_names_df = pd.DataFrame(brand_name_dic)
        brand_names_df = brand_names_df[["dso", "brand", "name"]]
        return brand_names_df
    all_dfs = pd.concat([manual_adds(), existing_df[['dso', 'brand', 'name']]])

    return all_dfs
