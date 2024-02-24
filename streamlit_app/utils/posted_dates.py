import re
from datetime import datetime, timedelta

def get_posted_dates(string):
    # Find all digits
    match = re.findall(r'\d+', string)
    if "today" in string.lower():
        return "Today"
    elif len(match) == 1 and match[0] != "30":
        today = datetime.today()
        # subtract the number of days from today
        date = today - timedelta(days=int(match[0]))
        return str(date.strftime("%Y-%m-%d"))
    else:
        return "30+ days ago"
