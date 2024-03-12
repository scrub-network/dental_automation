import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from utils.geomaps_api import geocode_locations_using_google
from utils.clean_description import clean_and_format_text
from utils.metrics import get_main_metrics
from utils.map_customization import color_scale, calculate_distance, create_custom_popup_job_posting
from utils.us_states_mapping import contains_zip_code, contains_city_name
from geopy.distance import geodesic
import streamlit_folium
import plotly.express as px
from datetime import datetime
from folium import Map, Marker, Icon, Popup
import re

def job_posting_containers(df):
    for ind, row in df.iterrows():
        employer = row['employer'].strip("(AI) ")
        valid_status = row['validity_status']
        with st.expander(f"**{employer}** - {row['location']} [{valid_status}]"):
            st.markdown(f"## [{row['job_title']}]({row['post_link']})")
            st.markdown(f"**Source:** {row['source']}")
            st.markdown(f"**Employer:** {row['employer']}")
            st.markdown(f"**Location:** {row['actual_address']}")
            st.markdown(f"**Date Posted:** {row['date_posted']}")
            st.markdown(f"**Job Type:** {row['job_type']}")
            description = clean_and_format_text(row['description'])
            st.markdown(f"**Description:** {description}")
            if row['days_of_week'] and not pd.isna(row['days_of_week']) and row['days_of_week'] != 'Not Provided':
                st.markdown(f"**Days of Week:** {row['days_of_week']}")
            if row['phone_number'] and not pd.isna(row['phone_number']):
                st.markdown(f"**Phone Number:** {row['phone_number']}")
            if row['email_address'] and not pd.isna(row['email_address']):
                st.markdown(f"**Email Address:** {row['email_address']}")
        st.write(" ")

# Streamlit page configuration
st.set_page_config(
    page_title="Job Postings",
    page_icon="📫",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("📫 Job Postings")

# Initialize connection
db_uri = st.secrets["db_uri"]
con = create_engine(db_uri)
df = pd.read_sql("select * from public.job_postings", con)
validation_df = pd.read_sql_query('select * from source.validations', con=con)
df = df.merge(validation_df, how='left', on=['job_id', 'post_link'])
df = df[['job_title', 'employer', 'employer_type',
         'location', 'state', 'date_posted',
         'job_type', 'description', 'post_link',
         'source', 'validity_status', 'expired_date', 
         'created_at']]

# If validty_status is Valid then make expired_date as None
df['expired_date'] = np.where(df['validity_status'] == 'Valid', None, df['expired_date'])

total_job_postings, total_job_postings_delta, total_job_postings_last_7_days, \
seven_days_ago_delta, total_unique_employers, total_unique_employers_delta = get_main_metrics(df)

st.write("# ")

c1, c2, c3 = st.columns(3)
with c1:
    st.metric(label="**Job Postings**", value=total_job_postings, delta=total_job_postings_delta)
with c2:
    st.metric(label="**Posted Within 7 Days**", value=total_job_postings_last_7_days, delta=seven_days_ago_delta)
with c3:
    st.metric(label="**Unique Employers**", value=total_unique_employers, delta=total_unique_employers_delta)

# Add Spacing
st.write("# ")
sql = text("SELECT * FROM source.source_private_practices")
existing_df = pd.read_sql(sql, con)
existing_df["latitude"] = existing_df["latitude"].astype(float)
existing_df["longitude"] = existing_df["longitude"].astype(float)
existing_df = existing_df.merge(validation_df, how='left', on=['job_id', 'post_link'])

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
today_date = pd.to_datetime('today').normalize()
us_mainland_df['date_posted_dt'] = pd.to_datetime(us_mainland_df['date_posted'])
us_mainland_df['date_posted_days'] = (today_date - us_mainland_df['date_posted_dt']).dt.days
us_mainland_df['normalized_days'] = us_mainland_df['date_posted_days'] / us_mainland_df['date_posted_days'].max()

# User Input
user_location = st.text_input("Enter your address or zip code:")

if user_location:
    # Radius Selection
    radius_selected = st.slider("Select a radius (in miles)", min_value=0, max_value=100, value=100, step=5, key="radius")

    user_lat, user_lon = geocode_locations_using_google(user_location)
    us_mainland_df['distance_from_user'] = us_mainland_df.apply(
        lambda row: calculate_distance(row, user_lat, user_lon), axis=1
    )
    us_mainland_df = us_mainland_df[us_mainland_df['distance_from_user'] <= radius_selected]

    # Displaying Nearby Practices
    us_mainland_df['color'] = us_mainland_df['normalized_days'].apply(color_scale).astype(str)

    # Clean up us_mainland_df
    map_df = us_mainland_df[['latitude', 'longitude', 'color']]
    map_df.reset_index(drop=True, inplace=True)

    # st.map(map_df, zoom=5, color='color')
    average_latitude = map_df['latitude'].dropna().mean()
    average_longitude = map_df['longitude'].dropna().mean()
    folium_map = Map(location=[average_latitude, average_longitude], zoom_start=6)

    for index, row in us_mainland_df.iterrows():
        custom_popup = create_custom_popup_job_posting(row['job_title'], row['employer'],
                                                       row['date_posted'], row['actual_address'],
                                                       row['website'], row['phone_number'],
                                                       row['email_address'])
        Marker(
            location=[row['latitude'], row['longitude']],
            popup=custom_popup,
            icon=Icon(color=row['color'])
        ).add_to(folium_map)

    # Display the map in Streamlit
    streamlit_folium.folium_static(folium_map, width=1000, height=500)

    st.write("Map showing posts with dates. Darker green indicates more recent posts, and lighter green indicates older posts.")
    us_mainland_df = us_mainland_df[['job_title', 'employer', 'location',
                                     'date_posted', 'job_type', 'description',
                                     'days_of_week', 'post_link', 'source',
                                     'actual_address', 'phone_number', 'email_address',
                                     'validity_status', 'expired_date', 'created_at']]

    # st.data_editor(us_mainland_df, num_rows="dynamic", hide_index=True)
    # Order by date_posted
    us_mainland_df = us_mainland_df.sort_values(by=['date_posted'], ascending=False)

else:
    us_mainland_df['color'] = us_mainland_df['normalized_days'].apply(color_scale)
    st.map(us_mainland_df, zoom=3.5, color='color')
    st.write("Map showing posts with dates. Darker green indicates more recent posts, and lighter green indicates older posts.")
    us_mainland_df = us_mainland_df[['job_title', 'employer', 'location',
                                     'state', 'date_posted', 'job_type',
                                     'description', 'days_of_week', 'post_link',
                                     'source', 'actual_address', 'phone_number',
                                     'email_address', 'validity_status', 'expired_date', 'created_at']]
    # st.data_editor(us_mainland_df, num_rows="dynamic", hide_index=True)
    # Order by date_posted
    us_mainland_df = us_mainland_df.sort_values(by=['date_posted'], ascending=False)


tab1, tab2 = st.tabs(["Job Postings", "Statistics"])
with tab1:
    job_posting_containers(us_mainland_df)

with tab2:
    # Convert date columns to datetime
    df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

    st.header('General Statistics')

    # Demonstrate Valid vs Expired/Broken Status
    c1, c2 = st.columns(2)
    with c1:
        st.metric(label="Valid Links Count", value=df[df['validity_status'] == 'Valid'].shape[0])
    with c2:
        st.metric(label="Expired/Broken Links Count", value=df[df['validity_status'] != 'Valid'].shape[0])

    # Divide the posting into groups: 1 week old, 1 month old, 3 months old, and older
    one_week_ago = pd.to_datetime('today') - pd.Timedelta(days=7)
    one_month_ago = pd.to_datetime('today') - pd.Timedelta(days=30)
    three_months_ago = pd.to_datetime('today') - pd.Timedelta(days=90)

    df["post_group"] = np.where(df['date_posted'] >= one_week_ago, '1 week old',
                                np.where(df['date_posted'] >= one_month_ago, '1 month old',
                                np.where(df['date_posted'] >= three_months_ago, '3 months old', 'older')))
    df["contains_zip_code"] = df['location'].apply(contains_zip_code)
    df["contains_city_name"] = df['location'].apply(contains_city_name)
    
    selected_group = st.selectbox('Select Post Group', ['All', 'Exclude Older'], key='post_group')

    col1, col2 = st.columns(2)
    with col1:
        st.subheader('Number of Postings by State')
        st.write("This bar graph shows the number of postings by state.")
        # Give selection option to pick all or exclude older
        if selected_group == 'Exclude Older':
            df = df[df['post_group'] != 'older']
        # Group by post_group and color them by post_group and order by total count
        df_state_count_by_post_group = df.groupby(['state', 'post_group']).size().reset_index(name='count')
        # Sort by count and post_group
        df_state_count_by_post_group = df_state_count_by_post_group.sort_values(by=['count', 'post_group'],
                                                                                ascending=[False, True])
        fig_state = px.bar(df_state_count_by_post_group, x='state', y='count',
                           color='post_group', labels={'x': 'State', 'y': 'Number of Postings'})
        st.plotly_chart(fig_state)

    with col2:
        # Bar graph
        st.subheader('Number of Postings by Source')
        st.write("This bar graph shows the number of postings by source.")
        if selected_group == 'Exclude Older':
            df = df[df['post_group'] != 'older']
        df_source_count_by_post_group = df.groupby(['source', 'post_group']).size().reset_index(name='count')
        df_source_count_by_post_group = df_source_count_by_post_group.sort_values(by=['count', 'post_group'],
                                                                                  ascending=[False, True])
        fig_source = px.bar(df_source_count_by_post_group, x='source', y='count',
                            color='post_group', labels={'x': 'Source', 'y': 'Number of Postings'})
        st.plotly_chart(fig_source)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Address Analysis")
        st.write("This table shows the percentage of addresses that contain a zip code and city name.")
        # Show in percentages which source has more addresses that contains zip code and city name
        df_address_analysis = df.groupby(['source', 'contains_zip_code', 'contains_city_name']).size().reset_index(name='count')
        df_address_analysis['percentage'] = df_address_analysis.groupby(['source'])['count'].apply(lambda x: 100 * x / float(x.sum()))
        df_address_analysis = df_address_analysis.sort_values(by=['source', 'contains_zip_code', 'contains_city_name'],
                                                              ascending=[True, False, False])
        df_address_analysis.reset_index(drop=True, inplace=True)
        st.dataframe(df_address_analysis)

    with col2:
        # If both checked, classify "Useful", if only one checked, classify "Partially Useful", if none checked, classify "Not Useful"
        st.subheader("Usefulness Classification")
        st.write("This table shows the percentage of addresses that are useful, partially useful, and not useful.")
        df['usefulness'] = np.where((df['contains_zip_code'] == True) & (df['contains_city_name'] == True), 'Useful',
                                    np.where((df['contains_zip_code'] == True) | (df['contains_city_name'] == True), 'Partially Useful','Not Useful'))
        df_usefulness_count = df.groupby(['source', 'usefulness']).size().reset_index(name='count')
        df_usefulness_count['percentage'] = df_usefulness_count.groupby(['source'])['count'].apply(lambda x: 100 * x / float(x.sum()))
        df_usefulness_count = df_usefulness_count.sort_values(by=['source', 'usefulness'], ascending=[True, True])
        df_usefulness_count.reset_index(drop=True, inplace=True)
        st.dataframe(df_usefulness_count)

    c1, c2, c3 = st.columns(3)
    with c1:
        # Create a bar chart
        colors = {'Useful': 'green', 'Partially Useful': '#1fd655', 'Not Useful': 'red'}
        fig_usefulness = px.bar(df_usefulness_count, x='source', y='count', color='usefulness',
                                labels={'x': 'Source', 'y': 'Number of Postings'}, color_discrete_map=colors)
        st.plotly_chart(fig_usefulness)
    with c3:
        # Show overall usefulness
        df_usefulness_overall = df.groupby(['usefulness']).size().reset_index(name='count')
        df_usefulness_overall['percentage'] = df_usefulness_overall['count'] / df_usefulness_overall['count'].sum() * 100
        st.dataframe(df_usefulness_overall)

    # Move contains_ columns up front
    df = df[['contains_zip_code', 'contains_city_name', 'location',
             'source', 'employer', 'date_posted', 'job_title',
             'employer_type', 'state', 'job_type', 'description',
             'post_link', 'validity_status', 'expired_date', 'created_at']]

    # To datetime
    df["expired_date"] = pd.to_datetime(df["expired_date"], errors='coerce')

    # Order by contains_ true
    df = df.sort_values(by=['contains_zip_code', 'contains_city_name', 'date_posted'],
                        ascending=[False, False, False])
    st.subheader("Raw Data")
    st.write("This table shows the raw data.")
    st.data_editor(df, num_rows="dynamic")
