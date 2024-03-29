import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from utils.geomaps_api import geocode_locations_using_google
from utils.clean_description import clean_and_format_text
from utils.metrics import get_main_metrics
from utils.map_customization import color_scale, calculate_distance, create_custom_popup_practice_search, \
                                    color_chooser, json_to_dataframe, find_dental_practices_with_details
from geopy.distance import geodesic
import streamlit_folium
from folium import Map, Marker, Icon, IFrame, Popup
import requests
import time

# Streamlit page configuration
st.set_page_config(
    page_title="Practice Search",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("🔎 Practices")

# Initialize connection
db_uri = st.secrets["db_uri"]
engine = create_engine(db_uri)

# Query and process job postings data
job_posting_sql = text("SELECT * FROM job_post_scraping.job_postings")
df_job_postings = pd.read_sql(job_posting_sql, engine)
df_job_postings = df_job_postings[['job_title', 'employer', 'employer_type', 'location', 'state', 'date_posted', 'job_type', 'description', 'post_link', 'source', 'created_at']]

# Query and get the dso_practices
dso_sql = text("SELECT * FROM practice.dso")
df_dso_practices = pd.read_sql(dso_sql, engine)
dso_df_original = df_dso_practices.copy()

# Display key metrics (assuming get_main_metrics function is defined)
total_job_postings, total_job_postings_delta, total_job_postings_last_7_days, seven_days_ago_delta, total_unique_employers, total_unique_employers_delta = get_main_metrics(df_job_postings)
c1, c2, c3 = st.columns(3)
with c1: st.metric(label="Job Postings", value=total_job_postings, delta=total_job_postings_delta)
with c2: st.metric(label="Posted Within 7 Days", value=total_job_postings_last_7_days, delta=seven_days_ago_delta)
with c3: st.metric(label="Unique Employers", value=total_unique_employers, delta=total_unique_employers_delta)

# Add Spacing
st.write("# ")

# Query data
regional_search_sql = text("SELECT * FROM practice.regional_search_practices")
existing_df = pd.read_sql(regional_search_sql, engine)
original_existing_df = existing_df.copy()
existing_df["latitude"] = existing_df["latitude"].astype(float)
existing_df["longitude"] = existing_df["longitude"].astype(float)

# Remove rows with latitude and longitude as null
existing_df.dropna(subset=['latitude', 'longitude'], inplace=True)

# Filtering and Processing Data for U.S. Mainland
min_latitude, max_latitude = 24.396308, 49.384358
min_longitude, max_longitude = -125.000000, -66.934570

us_mainland_df = existing_df[
    (existing_df['latitude'] >= min_latitude) & 
    (existing_df['latitude'] <= max_latitude) &
    (existing_df['longitude'] >= min_longitude) & 
    (existing_df['longitude'] <= max_longitude)
]

# Date Processing
today_date = pd.to_datetime('today').normalize() - pd.Timedelta(days=1)
us_mainland_df['created_at_dt'] = pd.to_datetime(us_mainland_df['created_at'])
us_mainland_df['created_at_days'] = (today_date - us_mainland_df['created_at_dt']).dt.days
us_mainland_df['normalized_days'] = us_mainland_df['created_at_days'] / us_mainland_df['created_at_days'].max()

tab1, tab2 = st.tabs(["Search by Location", "Raw Data"])

with tab1:
    # User Input
    user_location = st.text_input("Enter your address or zip code:")
    user_lat, user_lon = geocode_locations_using_google(user_location)

    # Radius Selection
    radius_selected = st.slider("Select a radius (in miles)", min_value=0, max_value=100, value=5, step=1, key="radius")

    # Displaying Nearby Practices
    us_mainland_df['color'] = us_mainland_df['normalized_days'].apply(color_scale).astype(str)

    # Clean up us_mainland_df
    # join on place_id
    df_dso_practices = df_dso_practices[['place_id', 'dso', 'phone', 'latitude', 'longitude', "name", "full_address", "site", "rating", "reviews", "location_link", "business_status"]]
    df_dso_practices.columns = ['place_id', 'dso', 'phone_number', 'latitude', 'longitude', "name", "address", "website", "rating", "total_ratings", "google_maps_url", "business_status"]

    # Change from float64 to object
    df_dso_practices['latitude'] = df_dso_practices['latitude'].astype(float)
    df_dso_practices['longitude'] = df_dso_practices['longitude'].astype(float)

    us_mainland_df = us_mainland_df.merge(df_dso_practices[['place_id', 'dso']], on='place_id', how='left')
    us_mainland_df = us_mainland_df.merge(df_dso_practices[['phone_number', 'dso']], how='left', on='phone_number')
    us_mainland_df = us_mainland_df.merge(df_dso_practices[['latitude', 'longitude', 'dso']], how='left', on=['latitude', 'longitude'])

    # Combine dso_x and dso_y
    us_mainland_df['dso_x'] = us_mainland_df['dso_x'].fillna(us_mainland_df['dso_y'])
    us_mainland_df['dso'] = us_mainland_df['dso_x'].fillna(us_mainland_df['dso_y'])
    us_mainland_df.drop(columns=['dso_x', 'dso_y'], inplace=True)
    us_mainland_df = us_mainland_df[['place_id', 'name', 'address', 'phone_number', 'website', 'rating', 'total_ratings', 'google_maps_url', 'business_status', 'dso', 'latitude', 'longitude', 'color']]

    # Join the rest of the df_dso_practices that don't overlap with us_mainland_df
    df_dso_practices = df_dso_practices[~df_dso_practices['place_id'].isin(us_mainland_df['place_id'].tolist())]
    df_dso_practices = df_dso_practices[~df_dso_practices['phone_number'].isin(us_mainland_df['phone_number'].tolist())]
    df_dso_practices = df_dso_practices[["place_id", "name", "address", "phone_number", "website", "rating", "total_ratings", "google_maps_url", "business_status", "dso", "latitude", "longitude"]]
    df_dso_practices["color"] = None

    us_mainland_df = pd.concat([us_mainland_df, df_dso_practices], ignore_index=True)

    if user_lat is not None and user_lon is not None:
        us_mainland_df['distance_from_user'] = us_mainland_df.apply(
            lambda row: calculate_distance(row, user_lat, user_lon), axis=1
        )
        us_mainland_df = us_mainland_df[us_mainland_df['distance_from_user'] <= radius_selected]
        zoom = 13
    else:
        zoom = 4.4

    # us_mainland_df = pd.concat([us_mainland_df, df_dso_practices], ignore_index=True)

    map_df = us_mainland_df[['latitude', 'longitude', 'color']]
    map_df.reset_index(drop=True, inplace=True)

    average_latitude = map_df['latitude'].dropna().mean()
    average_longitude = map_df['longitude'].dropna().mean()

    try:
        folium_map = Map(location=[average_latitude, average_longitude], zoom_start=zoom)
    except ValueError:
        average_latitude = user_lat
        average_longitude = user_lon
        folium_map = Map(location=[average_latitude, average_longitude], zoom_start=zoom)

    if user_lat is not None and user_lon is not None:
        for index, row in us_mainland_df.iterrows():
            # Only display practices with a valid latitude and longitude that are within the radius
            if pd.notna(row['latitude']) and pd.notna(row['longitude']) and row['distance_from_user'] <= radius_selected:
                custom_popup = create_custom_popup_practice_search(row['name'], row['address'], row['phone_number'], row['website'],
                                                    row['rating'], row['total_ratings'], row['google_maps_url'],
                                                    row['business_status'], row['dso'])

                Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=custom_popup,
                    icon=Icon(color=color_chooser(row['name'], row['address'], row['dso']))
                    # icon=Icon(color=row['color'])  # Assuming 'color' is defined in your DataFrame
                ).add_to(folium_map)

        # Display the map in Streamlit
        streamlit_folium.folium_static(folium_map, width=1000, height=500)

        with st.expander("View Nearby Practices"):
            us_mainland_df.reset_index(drop=True, inplace=True)
            st.data_editor(us_mainland_df, key="nearby_practices")

        generate_button = st.button("Generate", type="primary")
        st.warning("Generating will cost money. Please be careful.")
        if generate_button and user_lat is not None and user_lon is not None:
            inputted_loc = f"{user_lat},{user_lon}"
            api_key = st.secrets["google_maps_api_key"]
            json = find_dental_practices_with_details(api_key, inputted_loc, radius_selected * 1609.34, existing_df['place_id'].tolist())
            st.dataframe(json)
            df = json_to_dataframe(json)

            # Check if the values exist in the database by name and address. If not, add them to the database.
            name_address_tuples = [(name, address, google_maps_url) for name, address, google_maps_url in zip(df['name'], df['address'], df['google_maps_url'])]
            existing_name_address_tuples = [(name, address, google_maps_url) for name, address, google_maps_url in zip(original_existing_df['name'], original_existing_df['address'], original_existing_df['google_maps_url'])]

            new_practices = df[~df[['name', 'address']].apply(tuple, axis=1).isin(existing_name_address_tuples)]

            # Add created_at and updated_at
            now_datetime = pd.to_datetime('now')
            new_practices['created_at'] = now_datetime
            new_practices['updated_at'] = now_datetime

            engine = create_engine(get_source_database_url())
            new_practices.to_sql('regional_search_practices', schema="practice", con=engine, if_exists='append', index=False)
            st.balloons()
        elif generate_button and (user_lat is None or user_lon is None):
            st.error("Please enter a valid address or zip code.")

    with tab2:
        st.write("## Raw Data")
        selected_filter = st.selectbox("Select a filter", ["All (Private Practices + DSO)", "Private Practices", "DSO Practices"])
        private_practice_df = existing_df[~existing_df["place_id"].isin(dso_df_original["place_id"])]
        private_practice_df = private_practice_df[['place_id', 'name', 'address', 'phone_number', 'website',
                                   'rating', 'total_ratings', 'latitude', 'longitude',
                                   'business_status', 'google_maps_url', 'created_at']]

        dso_df_original = dso_df_original[['place_id', 'name', 'full_address', 'phone', 'site',
                                           'rating', 'reviews', 'latitude', 'longitude',
                                           'business_status', 'location_link', 'dso']]
        dso_df_original.columns = ['place_id', 'name', 'address', 'phone_number', 'website',
                                   'rating', 'total_ratings', 'latitude', 'longitude',
                                   'business_status', 'google_maps_url', 'dso']
        dso_df_original['created_at'] = None
        combined_df = pd.concat([private_practice_df, dso_df_original], ignore_index=True)
        if selected_filter == "All (Private Practices + DSO)":
            st.data_editor(combined_df, key="combined_df")
        elif selected_filter == "Private Practices":
            st.data_editor(private_practice_df, key="private_practice_df")
        elif selected_filter == "DSO Practices":
            st.data_editor(dso_df_original, key="dso_df_original")

        st.write("# ")
        st.divider()

        # Basic Statistics
        st.write("## Basic Statistics")
        col1, col2 = st.columns(2)
        with col1:
            st.write("### Private Practices")
            st.write(private_practice_df.describe())

        with col2:
            st.write("### DSO Practices")
            st.write(dso_df_original.describe())