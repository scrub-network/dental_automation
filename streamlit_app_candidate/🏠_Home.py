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
from streamlit import file_uploader
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import json

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Database connection
def get_db_connection():
    db_uri = st.secrets["db_uri"]
    engine = create_engine(db_uri)
    return engine

@st.cache_data(ttl=600, max_entries=20, show_spinner=False, persist=False)  # Shorter `ttl` for potentially sensitive or frequently updated data
def get_user_credentials():
    engine = get_db_connection()
    return pd.read_sql("SELECT * FROM streamlit_app_candidate.user_credentials", engine)

@st.cache_data(ttl=6000, max_entries=20, show_spinner=False, persist=False)  # Shorter `ttl` for potentially sensitive or frequently updated data
def get_dso_practices():
    engine = get_db_connection()
    return pd.read_sql("SELECT * FROM practice.dso", engine)

def create_account(username, password, first_name, last_name, email, user_df):
    engine = get_db_connection()
    hashed_password = hash_password(password)
    if user_df['id'].empty:
        user_id = 1
    else:
        user_id = user_df['id'].max() + 1
    user_data = {
        'id': [user_id],
        'username': [username],
        'password': [hashed_password],
        'first_name': [first_name],
        'last_name': [last_name],
        'email': [email],
        'created_at': [pd.Timestamp.now()],
        'resume_uploaded': [False]
    }
    user_df = pd.DataFrame(user_data)

    try:
        user_df.to_sql('user_credentials', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)
        return True
    except SQLAlchemyError as e:
        print(e)
        return False

def authenticate_user(username, password):
    engine = get_db_connection()
    success = False  # Default to failure

    # Check user credentials
    sql = """
    SELECT password FROM streamlit_app_candidate.user_credentials
    WHERE username = %s
    """
    user_df = pd.read_sql_query(sql, engine, params=[username])

    # Verify password and set success flag
    if not user_df.empty and user_df.iloc[0]['password'] == hash_password(password):
        success = True

    # Log the login attempt
    log_data = {
        'username': [username],
        'login_time': [pd.Timestamp.now()],
        'login_status': ['Success' if success else 'Failure']
    }
    log_df = pd.DataFrame(log_data)

    log_df.to_sql('login_log', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)

    # Make sure username is stored in session state
    if success:
        st.session_state['username'] = username

    return success

def log_activity(username, location_input, radius, user_lat, user_lon, total_results):
    engine = get_db_connection()
    activity_data = {
        'username': username,
        'location_input': location_input,
        'radius': radius,
        'user_lat': user_lat,
        'user_lon': user_lon,
        'total_results': total_results
    }
    activity_df = pd.DataFrame([activity_data])  # Ensure data is in a list to form a single-row DataFrame

    try:
        activity_df.to_sql('activity_log', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)
        return True
    except Exception as e:
        print(e)
        return False

def authenticate_with_google():
    # json_creds = st.secrets["json_cred"]
    json_creds = "/Volumes/Programming/scrub_network/dental_automation/streamlit_app_candidate/credentials/my_credential.json"
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(json_creds)
    if gauth.credentials is None or gauth.access_token_expired:
        # Authenticate if they're not there or expired
        gauth.LocalWebserverAuth()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    gauth.SaveCredentialsFile(json_creds)
    return GoogleDrive(gauth)

def send_resume_email(resume_file, user_df):
    sender_email = "anddy0622@gmail.com"
    password = st.secrets["smtp_password"]
    receiver_emails = ["anddy0622@gmail.com", "sean@scrubnetwork.com"]
    # receiver_email = "anddy0622@gmail.com"

    username = user_df['username'].values[0]
    first_name = user_df['first_name'].values[0]
    last_name = user_df['last_name'].values[0]
    email = user_df['email'].values[0]

    # Send email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, password)

    for receiver_email in receiver_emails:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = "[NEW RESUME UPLOADED] - SCRUB NETWORK"

        # Attach the resume file to the email
        msg.attach(MIMEText("Hello, \n\nA new resume has been uploaded by " + first_name + " " + last_name + " (" + username + ").\n\n"))
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(resume_file.getvalue())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename="resume.pdf")
        msg.attach(attachment)
        server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    print("Email sent successfully")

def update_user_credentials():
    # Load data from the database
    engine = get_db_connection()
    user_existing_df = pd.read_sql("SELECT * FROM streamlit_app_candidate.user_credentials", engine)
    return user_existing_df

# Streamlit UI for account creation and login
st.set_page_config(page_title="DSO", page_icon=":tooth:", layout="wide")
st.title("Scrub Network")

engine = get_db_connection()
df_dso_practices = get_dso_practices()  # Replace direct call with cached function
user_df = get_user_credentials()  # Replace direct call with cached function

authenticated = False
if 'authenticated' not in st.session_state:
    menu = ["Login", "Create Account"]
    choice = st.sidebar.radio("Menu", menu, horizontal=True)
    if choice == "Create Account":
        first_name = st.sidebar.text_input("First Name")
        st.session_state['first_name'] = first_name
        last_name = st.sidebar.text_input("Last Name")
        st.session_state['last_name'] = last_name
        email = st.sidebar.text_input("Email")
        st.session_state['email'] = email
        username = st.sidebar.text_input("Username")
        st.session_state['username'] = username
        password = st.sidebar.text_input("Password", type="password")
        st.session_state['password'] = password
        confirm_password = st.sidebar.text_input("Retype password", type="password")

        if st.sidebar.button("Create Account"):
            if password != confirm_password:
                st.error("Passwords do not match!")
            elif create_account(username, password, first_name, last_name, email, user_df):
                st.balloons()
            elif first_name == "" or last_name == "" or email == "" or username == "" or password == "":
                st.error("Please fill in all fields")
            elif username in user_df['username'].values:
                st.error("Username already exists")
            else:
                st.error("Failed to create account")
            user_df = get_user_credentials()  # Replace direct call with cached function

    elif choice == "Login":
        username = st.sidebar.text_input("Username")
        password = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Login"):
            if authenticate_user(username, password):
                st.session_state['authenticated'] = True
                st.balloons()
                st.session_state['username'] = username
                try:
                    st.session_state['resume_uploaded'] = user_df[user_df['username'] == username]['resume_uploaded'].values[0]
                except IndexError:
                    st.session_state['resume_uploaded'] = False
            # already authenticated
            elif st.session_state.get('authenticated'):
                pass
            else:
                st.error("Please check your username and password")
else:
    username = st.session_state['username']
    user_df = get_user_credentials()  # Replace direct call with cached function
    st.write("#### Welcome, ", st.session_state["username"], " 👋")

# st.write(st.session_state)

# Display the rest of the page only if the user is authenticated
if st.session_state.get('authenticated') and st.session_state['resume_uploaded']:
    user_name = st.session_state["username"]
    st.divider()

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
    radius_selected = st.slider("Select a radius (in miles)", min_value=0, max_value=100, value=50, step=1, key="radius")

    if user_lat is not None and user_lon is not None:
        us_mainland_df['distance_from_user'] = us_mainland_df.apply(
            lambda row: calculate_distance(row, user_lat, user_lon), axis=1
        )
        us_mainland_df = us_mainland_df[us_mainland_df['distance_from_user'] <= radius_selected]
        zoom = 9.5
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
        streamlit_folium.folium_static(folium_map, width=1300, height=500)

        # Log user activity
        log_activity(username, user_location, radius_selected, user_lat, user_lon, us_mainland_df.shape[0])

        with st.expander("View Nearby Practices"):
            us_mainland_df.reset_index(drop=True, inplace=True)
            us_mainland_df = us_mainland_df[["name", "full_address", "phone", "site", "rating",
                                             "reviews", "location_link", "business_status", "dso",
                                             "distance_from_user"]]
            st.data_editor(us_mainland_df, key="nearby_practices")
elif st.session_state.get('authenticated') and st.session_state['resume_uploaded'] == False:
    st.write("# ")
    st.write("### Please upload your resume to continue ✨")
    uploaded_file = st.file_uploader("Upload your resume", type=['pdf', 'docx'])
    # streamlit bar
    my_bar = st.progress(0)
    if uploaded_file is not None:
        send_resume_email(uploaded_file, user_df)
        my_bar.progress(60, "Please wait while we process your resume")

        existing_df = update_user_credentials()

        # Check if the current user is in user_df
        if username not in existing_df['username'].values:
            if user_df['id'].empty:
                user_id = 1
            else:
                user_id = user_df['id'].max() + 1
            user_data = {
                'id': [user_id],
                'username': [username],
                'password': [hash_password(st.session_state['password'])],
                'first_name': [st.session_state['first_name']],
                'last_name': [st.session_state['last_name']],
                'email': [st.session_state['email']],
                'created_at': [pd.Timestamp.now()],
                'resume_uploaded': [True]
            }
            row_df = pd.DataFrame(user_data)
            row_df.to_sql('user_credentials', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)
        else:
            existing_df.loc[existing_df['username'] == username, 'resume_uploaded'] = True
            existing_df.to_sql('user_credentials', con=engine, schema='streamlit_app_candidate', if_exists='replace', index=False)
        my_bar.progress(100)
        st.session_state['resume_uploaded'] = True
        st.rerun()