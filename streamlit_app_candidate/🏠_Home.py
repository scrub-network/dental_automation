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
import re

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Database connection
def get_db_connection():
    db_uri = st.secrets["db_uri"]
    engine = create_engine(db_uri)
    return engine

@st.cache_data(ttl=30, max_entries=20, show_spinner=False, persist=False)  # Shorter `ttl` for potentially sensitive or frequently updated data
def get_user_credentials():
    engine = get_db_connection()
    return pd.read_sql("SELECT * FROM streamlit_app_candidate.user_credentials", engine)

@st.cache_data(ttl=6000, max_entries=20, show_spinner=False, persist=False)  # Shorter `ttl` for potentially sensitive or frequently updated data
def get_dso_practices():
    engine = get_db_connection()
    df = pd.read_sql("SELECT * FROM practice.dso", engine)
    df.drop_duplicates(subset=['full_address'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def create_account(email, password, first_name, last_name, user_df):
    engine = get_db_connection()
    hashed_password = hash_password(password)
    if user_df['id'].empty:
        user_id = 1
    else:
        user_id = user_df['id'].max() + 1
    user_data = {
        'id': [user_id],
        'email': [email],
        'password': [hashed_password],
        'first_name': [first_name],
        'last_name': [last_name],
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

def authenticate_user(email, password):
    engine = get_db_connection()
    success = False  # Default to failure

    # Check user credentials
    sql = f"""
    SELECT password FROM streamlit_app_candidate.user_credentials
    WHERE email = '{email}'
    """
    user_df = pd.read_sql_query(sql, engine)

    # Verify password and set success flag
    if not user_df.empty and user_df.iloc[0]['password'] == hash_password(password):
        success = True

    # Log the login attempt
    log_data = {
        'email': [email],
        'login_time': [pd.Timestamp.now()],
        'login_status': ['Success' if success else 'Failure']
    }
    log_df = pd.DataFrame(log_data)

    log_df.to_sql('login_log', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)

    # Make sure email is stored in session state
    if success:
        st.session_state['email'] = email

    return success

def log_activity(email, location_input, radius, user_lat, user_lon, total_results):
    engine = get_db_connection()
    activity_data = {
        'email': email,
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
    receiver_emails = ["anddy0622@gmail.com"]#, "sean@scrubnetwork.com"]

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
        msg.attach(MIMEText("Hello, \n\nA new resume has been uploaded by the following user:<br> <br> " + "<b>First Name</b>: " + st.session_state['first_name'] +\
                            "\n<br> <b>Last Name</b>: " + st.session_state['last_name'] + "<br> \n<b>Email</b>: " + st.session_state['email'] + "\n\n", 'html'))
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(resume_file.getvalue())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=resume_file.name)
        msg.attach(attachment)
        server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    print("Email sent successfully")

def update_user_credentials():
    # Load data from the database
    engine = get_db_connection()
    user_existing_df = pd.read_sql("SELECT * FROM streamlit_app_candidate.user_credentials", engine)
    return user_existing_df

def validate_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, email):
        return True
    return False


# Streamlit UI for account creation and login
st.set_page_config(page_title="DSO", page_icon=":tooth:", layout="wide")
st.title("Scrub Network")

engine = get_db_connection()
df_dso_practices = get_dso_practices()  # Replace direct call with cached function
# user_df = get_user_credentials()  # Replace direct call with cached function

authenticated = False
if 'authenticated' not in st.session_state:
    # Description of the app and instructions
    st.markdown("""
    Welcome to Scrub Network! Tailor your dental career by filtering DSOs to find the right practices in your preferred location 🚀

### How to Use the App
1. **Sign Up/Login** 🔑
   - Register or log in and upload your resume to access our full range of features.

#####

2. **Find DSO Clinics** 🗺️
   - Search for DSO practices near you by entering your location.
   - Adjust the search radius to find the best matches.

#####

3. **Explore and Connect** 🌱
   - View detailed information about practices and available positions.
   - Connect with practices to take the next step in your career.

### Please Note
- **Session Management:** Refreshing the page will log you out, but no worries, you can easily log back in and pick up where you left off!
- **Privacy First:** Your data and privacy are of utmost importance to us. Rest assured, we handle your information with great care and confidentiality.

#####

Get started with Scrub Network and propel your dental career to new heights today! 🌟
    """)

    menu = ["Login", "Create Account"]
    choice = st.sidebar.radio("Menu", menu, horizontal=True)
    if choice == "Create Account":
        first_name = st.sidebar.text_input("First Name")
        st.session_state['first_name'] = first_name
        last_name = st.sidebar.text_input("Last Name")
        st.session_state['last_name'] = last_name
        email = st.sidebar.text_input("Email")
        st.session_state['email'] = email
        password = st.sidebar.text_input("Password", type="password")
        st.session_state['password'] = password
        confirm_password = st.sidebar.text_input("Retype password", type="password")

        if st.sidebar.button("Create Account"):
            user_df = get_user_credentials()  # Replace direct call with cached function
            if password != confirm_password:
                with st.sidebar:
                    st.error("Passwords do not match!")
            elif not validate_email(email):
                with st.sidebar:
                    st.error("Invalid email address")
            elif email in user_df['email'].values:
                with st.sidebar:
                    st.error("Email already exists")
            elif first_name.strip(" ") == "" or last_name.strip(" ") == "" or\
                 email.strip(" ") == "" or password.strip(" ") == "":
                with st.sidebar:
                    st.error("Please fill in all fields")
            elif create_account(email, password, first_name, last_name, user_df):
                # Direct user to login page
                if authenticate_user(email, password):
                    st.session_state['authenticated'] = True
                    st.session_state["resume_uploaded"] = False
                    st.rerun()
                    st.write("#### Welcome, ", first_name.capitalize(), " 👋")
            else:
                with st.sidebar:
                    st.error("Failed to create account")

    elif choice == "Login":
        user_df = get_user_credentials()  # Replace direct call with cached function
        email = st.sidebar.text_input("Email")
        password = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Login"):
            if authenticate_user(email, password):
                st.session_state['authenticated'] = True
                st.session_state['email'] = email
                st.session_state['first_name'] = user_df[user_df['email'] == email]['first_name'].values[0]
                st.session_state['last_name'] = user_df[user_df['email'] == email]['last_name'].values[0]
                try:
                    st.session_state['resume_uploaded'] = user_df[user_df['email'] == email]['resume_uploaded'].values[0]
                except IndexError:
                    st.session_state['resume_uploaded'] = False
                st.rerun()
                st.write("#### Welcome, ", first_name.capitalize(), " 👋")
            # already authenticated
            elif st.session_state.get('authenticated'):
                pass
            else:
                # Error on the sidebar
                with st.sidebar:
                    st.error("Please check your credentials")

user_df = get_user_credentials()  # Replace direct call with cached function
# Display the rest of the page only if the user is authenticated
apply_button = False
if st.session_state.get('authenticated') and st.session_state['resume_uploaded']:
    email = st.session_state["email"]
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
    radius_selected = st.slider("Search Radius (in miles)", min_value=0, max_value=100, value=50, step=1, key="radius")

    if user_lat is not None and user_lon is not None:
        us_mainland_df['distance_from_user'] = us_mainland_df.apply(
            lambda row: calculate_distance(row, user_lat, user_lon), axis=1
        )
        us_mainland_df = us_mainland_df[us_mainland_df['distance_from_user'] <= radius_selected]
        zoom = 9.5
    else:
        zoom = 4.4

    # Create a unique color for each dso
    unique_dso = us_mainland_df['dso'].unique()
    unique_dso = unique_dso[~pd.isnull(unique_dso)]
    distinct_colors = ["red", "blue", "green", "purple", "orange", "darkred", "lightred", "beige", "darkblue",\
                        "darkgreen", "cadetblue", "darkpurple", "white", "pink", "lightblue", "lightgreen", "gray"]
    dso_color_dict = dict(zip(unique_dso, distinct_colors))
    us_mainland_df['color'] = us_mainland_df['dso'].map(dso_color_dict)
    map_df = us_mainland_df[['latitude', 'longitude']]
    map_df.reset_index(drop=True, inplace=True)

    average_latitude = map_df['latitude'].dropna().mean()
    average_longitude = map_df['longitude'].dropna().mean()

    # Create true/false filter for dso color
    filter_by_dso = st.checkbox("Filter by DSO Type", value=False)
    if filter_by_dso:
        dso_filter = st.multiselect("Filter by DSO Type", unique_dso, default=unique_dso)
    else:
        dso_filter = unique_dso
    us_mainland_df = us_mainland_df[us_mainland_df['dso'].isin(dso_filter)]

    try:
        folium_map = Map(location=[average_latitude, average_longitude], zoom_start=zoom)
    except ValueError:
        average_latitude = user_lat
        average_longitude = user_lon
        folium_map = Map(location=[average_latitude, average_longitude], zoom_start=zoom)

    if user_lat is not None and user_lon is not None:
        # Create a distinct marker for the user's location
        Marker(
            location=[user_lat, user_lon],
            popup='Your Location',
            icon=Icon(color='black', icon='star', prefix='fa', icon_color='yellow', size=(24, 24))  # Customize this icon as per your preference
        ).add_to(folium_map)

        for index, row in us_mainland_df.iterrows():
            # Only display practices with a valid latitude and longitude that are within the radius
            if pd.notna(row['latitude']) and pd.notna(row['longitude']) and row['distance_from_user'] <= radius_selected:
                custom_popup = create_custom_popup_practice_search(row['name'], row['full_address'], row['phone'], row['site'],
                                                    row['rating'], row['reviews'], row['location_link'],
                                                    row['business_status'], row['dso'])
                if filter_by_dso:
                    color_param = row['color']
                else:
                    color_param = color_chooser(row['name'], row['full_address'], row['dso'])
                Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=custom_popup,
                    icon=Icon(color=color_param)  # Assuming 'color' is defined in your DataFrame
                ).add_to(folium_map)

        st.write("# ")

    apply_button = st.button("Apply 🚀", type="primary")

    def make_clickable(link):
        # target _blank to open new window
        # extract clickable text to display for your link
        text = link.split('=')[1]
        return f'<a target="_blank" href="{link}">{text}</a>'

    if apply_button:
        with st.spinner("Loading map..."):
            # Display the map in Streamlit
            streamlit_folium.folium_static(folium_map, width=1300, height=500)

            # Log user activity
            log_activity(email, user_location, radius_selected, user_lat, user_lon, us_mainland_df.shape[0])
        
            us_mainland_df.reset_index(drop=True, inplace=True)
            us_mainland_df = us_mainland_df[["name", "street", "city", "state", "phone", "site", "rating",
                                                "reviews", "location_link", "business_status", "dso",
                                                "distance_from_user"]]
            us_mainland_df["distance_from_user"] = us_mainland_df["distance_from_user"].round()
            us_mainland_df["business_status"] = us_mainland_df["business_status"].apply(lambda x: "✅ OPERATIONAL" if x == "OPERATIONAL" else "⏸️ CLOSED_TEMPORARILY" if x == "CLOSED_TEMPORARILY" else "❌ CLOSED_PERMANENTLY")

            # Sort the dataframe by distance from user
            us_mainland_df.sort_values(by="distance_from_user", inplace=True)

            st.dataframe(
                    us_mainland_df,
                    column_config={
                        "name": "Practice Name",
                        "rating": st.column_config.NumberColumn(
                            "Rating",
                            format="%f ⭐",
                        ),
                        "street": "Street Address",
                        "city": "City",
                        "state": "State",
                        "phone": "Phone",
                        "reviews": st.column_config.NumberColumn(
                            "Reviews",
                            format="%f reviews",
                        ),
                        "site": st.column_config.LinkColumn(
                            "site", width="medium", display_text="Visit Website"
                        ),
                        "location_link": st.column_config.LinkColumn(
                            "location_link", width="medium", display_text="View on Google Maps"
                        ),
                        "distance_from_user": st.column_config.LineChartColumn(
                            "distance_from_user", y_min=0, y_max=int(radius_selected)
                        ),
                        "distance_from_user": st.column_config.ProgressColumn(
                            "Distance from User",
                            format="%f miles",
                            min_value=0,
                            max_value=int(radius_selected),
                        ),
                        "dso": "DSO Type",
                        "business_status": st.column_config.SelectboxColumn(
                            "Business Status",
                            options=["✅ OPERATIONAL", "⏸️ CLOSED_TEMPORARILY", "❌ CLOSED_PERMANENTLY"],
                        ),
                    },
                    hide_index=True,
                )

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
        email = st.session_state['email']

        # Check if the current user is in user_df
        if email not in existing_df['email'].values and email != st.session_state['email']:
            if user_df['id'].empty:
                user_id = 1
            else:
                user_id = user_df['id'].max() + 1
            user_data = {
                'id': [user_id],
                'email': [st.session_state['email']],
                'password': [hash_password(st.session_state['password'])],
                'first_name': [st.session_state['first_name']],
                'last_name': [st.session_state['last_name']],
                'created_at': [pd.Timestamp.now()],
                'resume_uploaded': [True]
            }
            row_df = pd.DataFrame(user_data)
            row_df.to_sql('user_credentials', con=engine, schema='streamlit_app_candidate', if_exists='append', index=False)
        else:
            existing_df.loc[existing_df['email'] == email, 'resume_uploaded'] = True
            existing_df.to_sql('user_credentials', con=engine, schema='streamlit_app_candidate', if_exists='replace', index=False)
        my_bar.progress(100)
        st.session_state['resume_uploaded'] = True
        st.rerun()