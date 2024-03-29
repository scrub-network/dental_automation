import requests
from bs4 import BeautifulSoup
import re
from email_validator import validate_email, EmailNotValidError

def strip_after_domain(text):
    # Regex pattern to match .com, .net, .org, .edu, .gov followed by any characters
    pattern = r'(\.com|\.net|\.org|\.edu|\.gov)[^\s]*'

    # Replace the matched patterns with just the domain extension
    result = re.sub(pattern, r'\1', text)

    # Replace '.' with '-' if it comes before '@'
    result1, result2 = result.split('@')
    result1 = result1.replace('.', '-')
    result = result1 + '@' + result2

    return result

def extract_emails(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Refined regular expression for matching emails
        # This pattern attempts to exclude common phone number formats
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, soup.get_text())

        # Filter out emails that might be mistakenly captured phone numbers
        valid_emails = []
        for email in emails:
            # Strip any string that comes after '.com' or '.net' or '.org' or '.edu' or '.gov'
            email = strip_after_domain(email)

            # case for '1-201-461-4448' type
            if re.search(r'\d{1}-\d{3}-\d{3}-\d{4}', email):  # Pattern to detect phone numbers
                # strip out phone numbers
                email = re.sub(r'\d{1}-\d{3}-\d{3}-\d{4}', '', email)
                email = email.lstrip('0123456789').lower()
                try:
                    print(email)
                    valid = validate_email(email)
                    valid_emails.append(valid.email)
                except EmailNotValidError:
                    continue
            # for '201-461-4448' type
            elif re.search(r'\d{2,}-\d{2,}-\d{2,}', email):  # Pattern to detect phone numbers
                # strip out phone numbers
                email = re.sub(r'\d{2,}-\d{2,}-\d{2,}', '', email)
                email = email.lstrip('0123456789').lower()
                try:
                    print(email)
                    valid = validate_email(email)
                    valid_emails.append(valid.email)
                except EmailNotValidError:
                    continue
            # for '461-0701' type
            elif re.search(r'\d{3}-\d{4}', email):
                # strip out phone numbers
                email = re.sub(r'\d{3}-\d{4}', '', email)
                email = email.lstrip('0123456789').lower()
                try:
                    print(email)
                    valid = validate_email(email)
                    valid_emails.append(valid.email)
                except EmailNotValidError:
                    continue
            else:
                email = email.lstrip('0123456789').lower()
                try:
                    print(email)
                    # Validate the email format
                    valid = validate_email(email)
                    valid_emails.append(valid.email)
                except EmailNotValidError:
                    # Email not valid, skip
                    continue
        return valid_emails

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []

# Example Usage
import pandas as pd
practice_list = pd.read_csv('streamlit_app/utils/practice_list.csv')

email_df = pd.DataFrame(columns=['website', 'emails'])
for website in practice_list['website']:
    if pd.isna(website):
        continue
    print(website)
    emails = extract_emails(website)
    emails = list(set(emails))
    row_df = pd.DataFrame([[website, emails]], columns=['website', 'emails'])
    email_df = pd.concat([email_df, row_df])
    print(emails)
    print()

# Count the number of emails extracted
email_df.to_csv('streamlit_app/utils/emails.csv')
