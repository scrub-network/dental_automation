import googlemaps

def get_place_details(name, address, zipcode):
    # Initialize the Google Maps client
    gmaps = googlemaps.Client(key='AIzaSyA3gUG6-zGHiznAC6IJU6VUurMuajj8E2M')

    try:
        # Combine the inputs to form a specific query
        combined_query = f"{name} {address} {zipcode}"

        # Search for places using the combined query
        places_result = gmaps.places(query=combined_query)

        # Check if any results are found
        if places_result['results']:
            # Assuming the first result is the most relevant
            first_result = places_result['results'][0]
            place_id = first_result.get('place_id', None)
            location = first_result.get('geometry', {}).get('location', None)
            latitude = location.get('lat', None) if location else None
            longitude = location.get('lng', None) if location else None

            return {
                'Place ID': place_id,
                'Latitude': latitude,
                'Longitude': longitude
            }
        else:
            return {
                'Place ID': None,
                'Latitude': None,
                'Longitude': None
            }

    except Exception as e:
        print(f"Error processing query {combined_query}: {e}")
        return {
            'Place ID': None,
            'Latitude': None,
            'Longitude': None
        }
