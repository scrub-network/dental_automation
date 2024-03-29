import us
import re
import pandas as pd

def get_full_state_name(address):
    # Check if the address contains a full state name
    for state in us.STATES:
        if state.name in address:
            return state.name

    # Create a list of valid state abbreviations
    valid_state_abbr = [state.abbr for state in us.STATES]

    # Extract the state abbreviation using regex
    matches = re.findall(r'\b[A-Z]{2}\b', address)
    for match in matches:
        if match in valid_state_abbr:
            state = us.states.lookup(match)
            if state:
                return state.name
    return None

def contains_zip_code(s):
    # Regular expression for a five-digit zip code
    zip_code_pattern = r'\b\d{5}\b'
    # Search the string for the pattern
    return re.search(zip_code_pattern, s) is not None

def contains_city_name(address):
    # This regex pattern looks for a sequence of alphabetic characters that could be a city name
    # followed by a space and state abbreviation, which is typical in US addresses.
    pattern = r'\b([A-Za-z][a-zA-Z\s.-]*[A-Za-z])\b(?![\s]*\d)'

    list_of_states = [state.name for state in us.STATES] + [state.abbr for state in us.STATES] + ["USA", "US", "United States"]
    match = re.search(pattern, address)
    if match:
        city = match.group(1)
        if city not in list_of_states or city == "Washington, District of Columbia":
            return True
    return False

if __name__ != "__main__":
    # If the script is being imported, do not run the main function
    pass