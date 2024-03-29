import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils.geomaps_api import geocode_locations_using_google
from utils.map_customization import color_scale, calculate_distance, create_custom_popup_practice_search, \
                                    color_chooser, json_to_dataframe, find_dental_practices_with_details
from folium import Map, Marker, Icon, IFrame, Popup
import streamlit_folium
import pandas as pd
import hashlib

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Database connection
def get_db_connection():
    db_uri = st.secrets["db_uri"]
    engine = create_engine(db_uri)
    return engine

def create_account(username, password, first_name, last_name, email):
    engine = get_db_connection()
    hashed_password = hash_password(password)
    with engine.connect() as connection:
        try:
            sql = """
            INSERT INTO streamlit_app_candidate.user_credentials (username, password, first_name, last_name, email)
            VALUES (%s, %s, %s, %s, %s)
            """
            connection.execute(sql, (username, hashed_password, first_name, last_name, email))
            return True
        except SQLAlchemyError as e:
            print(e)
            return False


def authenticate_user(username, password):
    engine = get_db_connection()
    success = False  # Default to failure
    with engine.connect() as connection:
        # Check user credentials
        sql = """
        SELECT password FROM streamlit_app_candidate.user_credentials
        WHERE username = %s
        """
        user = connection.execute(sql, (username,)).fetchone()
        
        # Verify password and set success flag
        if user and user['password'] == hash_password(password):
            success = True

        # Log the login attempt
        log_sql = """
        INSERT INTO streamlit_app_candidate.login_log (username, login_time, login_status)
        VALUES (%s, CURRENT_TIMESTAMP, %s)
        """
        connection.execute(log_sql, (username, 'Success' if success else 'Failure'))

        return success

# Streamlit UI for account creation and login
st.set_page_config(page_title="DSO", page_icon=":tooth:", layout="wide")
st.title("Scrub Network")

menu = ["Login", "Create Account"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Create Account":
    first_name = st.sidebar.text_input("First Name")
    last_name = st.sidebar.text_input("Last Name")
    email = st.sidebar.text_input("Email")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    confirm_password = st.sidebar.text_input("Retype password", type="password")
    
    if st.sidebar.button("Create Account"):
        if password != confirm_password:
            st.error("Passwords do not match!")
        elif create_account(username, password, first_name, last_name, email):
            st.success("Account created successfully!")
            # clear success message after 5 seconds
            st.experimental_rerun()
        else:
            st.error("Failed to create account")
            st.experimental_rerun()

elif choice == "Login":
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        if authenticate_user(username, password):
            st.session_state['authenticated'] = True
            st.success("Logged in successfully!")
        else:
            st.error("Failed to log in")

# Display the rest of the page only if the user is authenticated
if st.session_state.get('authenticated'):
    # Initialize connection
    engine = get_db_connection()
    user_df = pd.read_sql("SELECT * FROM streamlit_app_candidate.user_credentials", engine)
    user_name = user_df[user_df['username'] == username]['first_name'].values[0]
    st.write("#### Welcome, ", user_name, " 👋")
    st.divider()

    # Query data
    df_dso_practices = pd.read_sql("SELECT * FROM practice.dso", engine)

    # Filtering and Processing Data for U.S. Mainland
    min_latitude, max_latitude = 24.396308, 49.384358
    min_longitude, max_longitude = -125.000000, -66.934570

    us_mainland_df = df_dso_practices[
        (df_dso_practices['latitude'] >= min_latitude) & 
        (df_dso_practices['latitude'] <= max_latitude) &
        (df_dso_practices['longitude'] >= min_longitude) & 
        (df_dso_practices['longitude'] <= max_longitude)
    ]

    # User Input
    user_location = st.text_input("Enter your full_address or zip code:")
    user_lat, user_lon = geocode_locations_using_google(user_location)

    # Radius Selection
    radius_selected = st.slider("Select a radius (in miles)", min_value=0, max_value=100, value=5, step=1, key="radius")

    if user_lat is not None and user_lon is not None:
        us_mainland_df['distance_from_user'] = us_mainland_df.apply(
            lambda row: calculate_distance(row, user_lat, user_lon), axis=1
        )
        us_mainland_df = us_mainland_df[us_mainland_df['distance_from_user'] <= radius_selected]
        zoom = 13
    else:
        zoom = 4.4

    map_df = us_mainland_df[['latitude', 'longitude']]
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
                custom_popup = create_custom_popup_practice_search(row['name'], row['full_address'], row['phone'], row['site'],
                                                    row['rating'], row['reviews'], row['location_link'],
                                                    row['business_status'], row['dso'])
                Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=custom_popup,
                    icon=Icon(color=color_chooser(row['name'], row['full_address'], row['dso']))
                    # icon=Icon(color=row['color'])  # Assuming 'color' is defined in your DataFrame
                ).add_to(folium_map)

        # Display the map in Streamlit
        streamlit_folium.folium_static(folium_map, width=1000, height=500)

        with st.expander("View Nearby Practices"):
            us_mainland_df.reset_index(drop=True, inplace=True)
            us_mainland_df = us_mainland_df[["name", "full_address", "phone", "site", "rating",
                                             "reviews", "location_link", "business_status", "dso",
                                             "distance_from_user"]]
            st.data_editor(us_mainland_df, key="nearby_practices")