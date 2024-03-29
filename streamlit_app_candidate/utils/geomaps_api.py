import googlemaps
import streamlit as st

def geocode_locations_using_google(df):

    # Initialize the Google Maps client
    google_api_key = st.secrets["google_api_key"]
    gmaps = googlemaps.Client(key=google_api_key)
    # Check if the input is a single string (user's address or zip code)
    if isinstance(df, str):
        try:
            # Geocode the single location
            geocode_result = gmaps.geocode(df)
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
            return lat, lng
        except Exception as e:
            print(f"Error processing {df}: {e}")
            return None, None

    # If df is a DataFrame, process it as before
    elif isinstance(df, pd.DataFrame):

        # Create empty lists to store latitudes, longitudes, addresses, phone numbers, and websites
        latitudes = []
        longitudes = []
        addresses = []
        phone_numbers = []
        websites = []

        # Iterate over the rows of the dataframe to geocode each location
        for index, row in df.iterrows():
            # Combine employer, location, and state for more accurate geocoding
            query = f"{row['employer']}, {row['location']}, {row['state']}"

            try:
                # Use the Google Maps client to geocode the location
                geocode_result = gmaps.geocode(query)
                
                # Extract latitude, longitude, and formatted address from the geocode result
                lat = geocode_result[0]['geometry']['location']['lat']
                lng = geocode_result[0]['geometry']['location']['lng']
                address = geocode_result[0]['formatted_address']
                
                # Use the Place Details request to fetch additional details using place_id
                place_id = geocode_result[0]['place_id']
                details_result = gmaps.place(place_id=place_id)
                
                # Extract phone number and website from the details result (if available)
                phone_number = details_result['result'].get('formatted_phone_number', None)
                website = details_result['result'].get('website', None)

                latitudes.append(lat)
                longitudes.append(lng)
                addresses.append(address)
                phone_numbers.append(phone_number)
                websites.append(website)

            except Exception as e:
                print(f"Error processing {query}: {e}")
                latitudes.append(None)
                longitudes.append(None)
                addresses.append(None)
                phone_numbers.append(None)
                websites.append(None)

        # Add latitudes, longitudes, addresses, phone numbers, and websites to the dataframe
        df['latitude'] = latitudes
        df['longitude'] = longitudes
        df['actual_address'] = addresses
        df['phone_number'] = phone_numbers
        df['website'] = websites
        df['email_address'] = None  # Initialize email_address column with None
        
        return df
