import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from utils.geomaps_api import geocode_locations_using_google
from utils.clean_description import clean_and_format_text
from utils.metrics import get_main_metrics
from geopy.distance import geodesic
import streamlit_folium
import plotly.express as px
from datetime import datetime
from folium import Map, Marker, Icon, IFrame, Popup
import re

# Streamlit page configuration
st.set_page_config(
    page_title="Job Postings",
    page_icon="📫",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("📫 Job Postings")

# Function Definitions

def color_scale(value):
    """Maps a normalized value to a scale from dark green to white."""
    # All components (R, G, B) increase proportionally from dark green to white
    intensity = int(255 * value)  # Intensity for red and blue components
    green_intensity = int(100 + 155 * value)  # Green starts from a darker shade (100) and increases to 255
    return f'#{intensity:02x}{green_intensity:02x}{intensity:02x}'

def calculate_distance(row, user_lat, user_lon):
    """Calculates the geodesic distance between two points."""
    practice_location = (row['latitude'], row['longitude'])
    user_location = (user_lat, user_lon)
    return geodesic(practice_location, user_location).miles

def convert_df(df):
    """Converts a DataFrame to CSV."""
    return df.to_csv(index=False).encode('utf-8')

def job_posting_containers(df):
    for ind, row in df.iterrows():
        employer = row['employer'].strip("(AI) ")
        with st.expander(f"**{employer}** - {row['location']}"):
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

def create_custom_popup(job_title, employer, date_posted, address, website, phone_number, email, width=300, height=200):
    # HTML content for the popup
    html_content = f'''
    <div style="font-size: 12pt; font-family: Arial;">
        <h3>{job_title}</h3>
        <b>Employer</b>: {employer}<br>
        <b>Posted</b>: {date_posted}<br>
        <b>Address</b>: {address}<br>
    '''
    if website and not pd.isna(website):
        html_content += f'<b>Website</b>: <a href="{website}" target="_blank">{website}</a><br>'
    if phone_number and not pd.isna(phone_number):
        html_content += f'<b>Phone Number</b>: {phone_number}<br>'
    if email and not pd.isna(email):
        html_content += f'<b>Email</b>: {email}<br>'
    html_content += '</div>'

    # Create IFrame with HTML content
    iframe = IFrame(html_content, width=width, height=height)
    return Popup(iframe, max_width=width)

def contains_zip_code(s):
    # Regular expression for a five-digit zip code
    zip_code_pattern = r'\b\d{5}\b'
    # Search the string for the pattern
    return re.search(zip_code_pattern, s) is not None

def contains_city_name(address):
    # This regex pattern looks for a sequence of alphabetic characters that could be a city name
    # followed by a space and state abbreviation, which is typical in US addresses.
    pattern = r'\b([A-Za-z][a-zA-Z\s.-]*[A-Za-z])\b(?![\s]*\d)'

    # Use re.search() to check if the pattern is found in the address
    return bool(re.search(pattern, address))


# Initialize connection
db_uri = st.secrets["db_uri"]
con = create_engine(db_uri)
sql = text("SELECT * FROM public.job_postings")
df = pd.read_sql(sql, con)
df = df[['job_title', 'employer', 'employer_type', 'location', 'state', 'date_posted', 'job_type', 'description', 'post_link', 'source', 'created_at']]

total_job_postings, total_job_postings_delta, total_job_postings_last_7_days, seven_days_ago_delta, total_unique_employers, total_unique_employers_delta = get_main_metrics(df)

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
        custom_popup = create_custom_popup(row['job_title'], row['employer'], row['date_posted'], row['actual_address'],
                                        row['website'], row['phone_number'], row['email_address'])
        Marker(
            location=[row['latitude'], row['longitude']],
            popup=custom_popup,
            icon=Icon(color=row['color'])
        ).add_to(folium_map)

    # Display the map in Streamlit
    streamlit_folium.folium_static(folium_map, width=1000, height=500)

    st.write("Map showing posts with dates. Darker green indicates more recent posts, and lighter green indicates older posts.")
    us_mainland_df = us_mainland_df[['job_title', 'employer', 'location', 'date_posted', 'job_type', 'description', 'days_of_week', 'post_link', 'source', 'actual_address', 'phone_number', 'email_address', 'created_at']]
    # st.data_editor(us_mainland_df, num_rows="dynamic", hide_index=True)
    # Order by date_posted
    us_mainland_df = us_mainland_df.sort_values(by=['date_posted'], ascending=False)

else:
    us_mainland_df['color'] = us_mainland_df['normalized_days'].apply(color_scale)
    st.map(us_mainland_df, zoom=3.5, color='color')
    st.write("Map showing posts with dates. Darker green indicates more recent posts, and lighter green indicates older posts.")
    us_mainland_df = us_mainland_df[['job_title', 'employer', 'location', 'state', 'date_posted', 'job_type', 'description', 'days_of_week', 'post_link', 'source', 'actual_address', 'phone_number', 'email_address', 'created_at']]
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

    # Divide the posting into groups: 1 week old, 1 month old, 3 months old, and older
    one_week_ago = pd.to_datetime('today') - pd.Timedelta(days=7)
    one_month_ago = pd.to_datetime('today') - pd.Timedelta(days=30)
    three_months_ago = pd.to_datetime('today') - pd.Timedelta(days=90)

    df["post_group"] = np.where(df['date_posted'] >= one_week_ago, '1 week old', np.where(df['date_posted'] >= one_month_ago, '1 month old', np.where(df['date_posted'] >= three_months_ago, '3 months old', 'older')))
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
        df_state_count_by_post_group = df_state_count_by_post_group.sort_values(by=['count', 'post_group'], ascending=[False, True])
        fig_state = px.bar(df_state_count_by_post_group, x='state', y='count', color='post_group', labels={'x': 'State', 'y': 'Number of Postings'})
        st.plotly_chart(fig_state)

    with col2:
        # Bar graph
        st.subheader('Number of Postings by Source')
        st.write("This bar graph shows the number of postings by source.")
        if selected_group == 'Exclude Older':
            df = df[df['post_group'] != 'older']
        df_source_count_by_post_group = df.groupby(['source', 'post_group']).size().reset_index(name='count')
        df_source_count_by_post_group = df_source_count_by_post_group.sort_values(by=['count', 'post_group'], ascending=[False, True])
        fig_source = px.bar(df_source_count_by_post_group, x='source', y='count', color='post_group', labels={'x': 'Source', 'y': 'Number of Postings'})
        st.plotly_chart(fig_source)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Address Analysis")
        st.write("This table shows the percentage of addresses that contain a zip code and city name.")
        # Show in percentages which source has more addresses that contains zip code and city name
        df_address_analysis = df.groupby(['source', 'contains_zip_code', 'contains_city_name']).size().reset_index(name='count')
        df_address_analysis['percentage'] = df_address_analysis.groupby(['source'])['count'].apply(lambda x: 100 * x / float(x.sum()))
        df_address_analysis = df_address_analysis.sort_values(by=['source', 'contains_zip_code', 'contains_city_name'], ascending=[True, False, False])
        df_address_analysis.reset_index(drop=True, inplace=True)
        st.dataframe(df_address_analysis)

    with col2:
        # If both checked, classify "Useful", if only one checked, classify "Partially Useful", if none checked, classify "Not Useful"
        st.subheader("Usefulness Classification")
        st.write("This table shows the percentage of addresses that are useful, partially useful, and not useful.")
        df['usefulness'] = np.where((df['contains_zip_code'] == True) & (df['contains_city_name'] == True), 'Useful', np.where((df['contains_zip_code'] == True) | (df['contains_city_name'] == True), 'Partially Useful', 'Not Useful'))
        df_usefulness_count = df.groupby(['source', 'usefulness']).size().reset_index(name='count')
        df_usefulness_count['percentage'] = df_usefulness_count.groupby(['source'])['count'].apply(lambda x: 100 * x / float(x.sum()))
        df_usefulness_count = df_usefulness_count.sort_values(by=['source', 'usefulness'], ascending=[True, True])
        df_usefulness_count.reset_index(drop=True, inplace=True)
        st.dataframe(df_usefulness_count)

        # Show overall usefulness
        df_usefulness_overall = df.groupby(['usefulness']).size().reset_index(name='count')
        df_usefulness_overall['percentage'] = df_usefulness_overall['count'] / df_usefulness_overall['count'].sum() * 100
        st.dataframe(df_usefulness_overall)
        

    # Move contains_ columns up front
    df = df[['contains_zip_code', 'contains_city_name', 'location', 'source', 'employer', 'date_posted', 'job_title', 'employer_type', 'state', 'job_type', 'description', 'post_link', 'created_at']]
    # Order by contains_ true
    df = df.sort_values(by=['contains_zip_code', 'contains_city_name', 'date_posted'], ascending=[False, False, False])
    st.subheader("Raw Data")
    st.write("This table shows the raw data.")
    st.data_editor(df, num_rows="dynamic")
    