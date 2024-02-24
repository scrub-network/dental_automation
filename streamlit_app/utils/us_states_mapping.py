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