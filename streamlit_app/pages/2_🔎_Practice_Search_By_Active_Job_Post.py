import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from utils.geomaps_api import geocode_locations_using_google
from utils.clean_description import clean_and_format_text
from utils.metrics import get_main_metrics
from geopy.distance import geodesic
import streamlit_folium
from folium import Map, Marker, Icon, IFrame, Popup

# Streamlit page configuration
st.set_page_config(
    page_title="Search Private Practices",
    page_icon=":Magnifying Glass Tilted Right:",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("🔎 Search Private Practices")

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

tab1, tab2 = st.tabs(["Main", "Statistics"])
with tab1:
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
        job_posting_containers(us_mainland_df)

    else:
        us_mainland_df['color'] = us_mainland_df['normalized_days'].apply(color_scale)
        st.map(us_mainland_df, zoom=3.5, color='color')
        st.write("Map showing posts with dates. Darker green indicates more recent posts, and lighter green indicates older posts.")
        us_mainland_df = us_mainland_df[['job_title', 'employer', 'location', 'state', 'date_posted', 'job_type', 'description', 'days_of_week', 'post_link', 'source', 'actual_address', 'phone_number', 'email_address', 'created_at']]
        # st.data_editor(us_mainland_df, num_rows="dynamic", hide_index=True)
        # Order by date_posted
        us_mainland_df = us_mainland_df.sort_values(by=['date_posted'], ascending=False)
        job_posting_containers(us_mainland_df)

with tab2:
    st.data_editor(df, num_rows="dynamic", hide_index=True)