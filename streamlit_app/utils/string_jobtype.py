def extract_jobtype(string):
    # If there is "full" and "part" then return "Full Time, Part Time"
    # If there is "full" then return "Full Time"
    # If there is "part" then return "Part Time"
    # If there is "contract" then return "Contract"
    # If there is "temporary" then return "Temporary"
    if "full" in string.lower() and "part" in string.lower():
        return "Full Time, Part Time"
    elif "full" in string.lower():
        return "Full Time"
    elif "part" in string.lower():
        return "Part Time"
    elif "contract" in string.lower():
        return "Contract"
    elif "temporary" in string.lower():
        return "Temporary"
    else:
        return ""