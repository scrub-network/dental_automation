import us
import re
import pandas as pd

def get_full_state_name(address):
    # Check if the address contains a full state name
    for state in us.STATES:
        if state.name in address:
            return state.name

    # If not, then extract the state abbreviation using regex
    match = re.search(r'\b[A-Z]{2}\b', address)
    if match:
        state_abbr = match.group(0)
        state = us.states.lookup(state_abbr)
        if state:
            return state.name
    return None

