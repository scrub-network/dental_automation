import streamlit as st
import pandas as pd
import numpy as np
import re
from utils.clean_description import clean_and_format_text
from geopy.distance import geodesic
from folium import IFrame, Popup

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

def create_custom_popup_practice_search(name, address, phone_number, website, rating, total_ratings, google_maps_url, business_status, dso, width=300, height=200):
    # HTML content for the popup
    html_content = f'''
    <div style="font-size: 12pt; font-family: Arial;">
        <h3>{name}</h3>
        <b>Address</b>: {address}<br>
        <b>Status</b>: {business_status}<br>
        <b>Google Maps</b>: <a href="{google_maps_url}" target="_blank">View on Google Maps</a><br>
        <b>Rating</b>: {rating} ({total_ratings} ratings)<br>
    '''
    if website and not pd.isna(website):
        html_content += f'<b>Website</b>: <a href="{website}" target="_blank">{website}</a><br>'
    if phone_number and not pd.isna(phone_number):
        html_content += f'<b>Phone Number</b>: {phone_number}<br>'
    if dso and not pd.isna(dso):
        html_content += f'<b>DSO</b>: {dso}<br>'
    html_content += '</div>'

    # Create IFrame with HTML content
    iframe = IFrame(html_content, width=width, height=height)
    return Popup(iframe, max_width=width)

def create_custom_popup_job_posting(job_title, employer, date_posted, address, website, phone_number, email, width=300, height=200):
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

def find_dental_practices_with_details(api_key, location, radius, existing_place_ids):
    """
    Find nearby dental practices within a specified radius of the given location and fetch their details.

    Parameters:
    - api_key: Your Google Places API key.
    - location: The latitude/longitude around which to search for dental practices.
    - radius: The radius (in meters) within which to search.
    """
    # Function to fetch detailed information about a place using its place_id
    def get_place_details(place_id):
        details_url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "fields": "name,formatted_phone_number,website",
            "key": api_key
        }

        response = requests.get(details_url, params=params)
        if response.status_code == 200:
            return response.json().get('result', {})
        else:
            print(f"Error occurred: {response.status_code}")
            return {}

    # Finding nearby dental practices
    base_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": location,
        "radius": radius,
        "type": "dentist",
        "keyword": "dentist",
        "key": api_key
    }

    detailed_results = []
    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            practices = data.get('results', [])

            # Fetch and append details for each practice
            for practice in practices:
                practice_place_id = practice.get('place_id')
                if practice_place_id not in existing_place_ids:
                    details = get_place_details(practice_place_id)
                    practice.update(details)
                    detailed_results.append(practice)

            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
            params['pagetoken'] = next_page_token
            time.sleep(2)
        else:
            print(f"Error occurred: {response.status_code}")
            break

    return detailed_results

def json_to_dataframe(json_data):
    column_names = [
        "name", "address", "phone_number", "website", "rating", "total_ratings",
        "place_id", "latitude", "longitude", "business_status", "types",
        "opening_hours", "plus_code", "google_maps_url", "icon_url", "price_level"
    ]
    data = []
    for practice in json_data:
        row = [
            practice.get('name', ''),
            practice.get('vicinity', ''),
            practice.get('formatted_phone_number', ''),
            practice.get('website', ''),
            practice.get('rating', ''),
            practice.get('user_ratings_total', ''),
            practice.get('place_id', ''),
            practice.get('geometry', {}).get('location', {}).get('lat', ''),
            practice.get('geometry', {}).get('location', {}).get('lng', ''),
            practice.get('business_status', ''),
            ', '.join(practice.get('types', [])),
            str(practice.get('opening_hours', {}).get('open_now', '')),
            practice.get('plus_code', {}).get('compound_code', ''),
            f"https://www.google.com/maps/place/?q=place_id:{practice.get('place_id', '')}",
            practice.get('icon', ''),
            practice.get('price_level', '')
        ]
        data.append(row)
    return pd.DataFrame(data, columns=column_names)

def color_chooser(name, address, category):
    if type(category) != float and category is not None:
        return 'blue'
    else:
        return 'red'

if __name__ != "__main__":
    # If the script is being imported, do not run the main function
    pass