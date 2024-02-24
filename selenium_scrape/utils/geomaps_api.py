import googlemaps
import pandas as pd
import re  # Regular expression library for extracting zip code

def geocode_locations_using_google(df):
    # Initialize the Google Maps client
    gmaps = googlemaps.Client(key='AIzaSyA3gUG6-zGHiznAC6IJU6VUurMuajj8E2M')

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

    elif isinstance(df, pd.DataFrame):
        addresses = []
        phone_numbers = []
        zipcodes = []  # List to store zip codes

        for index, row in df.iterrows():
            print(f"Processing {index + 1} of {len(df)}")
            query = f"{row['address']}"

            try:
                geocode_result = gmaps.geocode(query)
                address = geocode_result[0]['formatted_address']

                # Extract zip code using regular expression
                zip_code_match = re.search(r'\b\d{5}\b', address)
                zip_code = zip_code_match.group(0) if zip_code_match else None

                place_id = geocode_result[0]['place_id']
                details_result = gmaps.place(place_id=place_id)

                phone_number = details_result['result'].get('formatted_phone_number', None)

                addresses.append(address)
                phone_numbers.append(phone_number)
                zipcodes.append(zip_code)  # Add zip code to the list

            except Exception as e:
                print(f"Error processing {query}: {e}")
                addresses.append(None)
                phone_numbers.append(None)
                zipcodes.append(None)

        df['actual_address'] = addresses
        df['phone'] = phone_numbers
        df['zip_code'] = zipcodes  # Add the zip code column to the dataframe

        return df
