import us
import re
import pandas as pd

def get_full_state_name(address):

    # Check if the address contains a full state name
    state_names = [state.name.lower() for state in us.STATES]
    for state in state_names:
        if state in address.lower():
            return state.title()

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