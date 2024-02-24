import googlemaps
import re

def get_practice_info_using_google(address):
    # Initialize the Google Maps client
    gmaps = googlemaps.Client(key='AIzaSyA3gUG6-zGHiznAC6IJU6VUurMuajj8E2M')

    try:
        # Search for places using the provided address
        places_result = gmaps.places(query=address)

        if len(places_result['results']) > 0:
            result = places_result['results'][0]
            practice_name = result.get('name', None)
            formatted_address = result.get('formatted_address', None)
            place_id = result.get('place_id', None)  # Extracting the place_id

            # Extract zip code from the address
            zip_code_match = re.search(r'\b\d{5}\b', formatted_address)
            zip_code = zip_code_match.group(0) if zip_code_match else None

            return {
                'Practice Name': practice_name,
                'Address': formatted_address,
                'Zip Code': zip_code,
                'Place ID': place_id  # Adding place_id to the returned information
            }
        else:
            return {
                'Practice Name': None,
                'Address': None,
                'Zip Code': None,
                'Place ID': None
            }

    except Exception as e:
        print(f"Error processing address {address}: {e}")
        return {
            'Practice Name': None,
            'Address': None,
            'Zip Code': None,
            'Place ID': None
        }

