import pandas as pd

def north_american_dental_group():
    from bs4 import BeautifulSoup
    import requests
    url = "https://nadentalgroup.com/north-american-dental-group-brands/"
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, "lxml")
    li_objects = soup.find_all("li")

    li_texts = []
    for ind, li in enumerate(li_objects):
        if ind > 8 and li.text != "":
            li_texts.append(li.text.replace("™",""))

    return li_texts

def scraped_dso_databasea():
    from sqlalchemy import create_engine, text
    engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
    sql = "select * from dso_scraping.dso_practices"
    exising_df = pd.read_sql(sql, con=engine)
    brand_names = exising_df["brand"].unique().tolist()
    practice_names = exising_df["name"].unique().tolist()

    # Remove any NoneType objects
    brand_names = [i for i in brand_names if i is not None]
    practice_names = [i for i in practice_names if i is not None]

    names = list(set(brand_names)) + list(set(practice_names))

    return names

def manual_adds():
    return ["Aspen", "Brident", "DentalWorks", "InterDent",
            "Western Dental", "Guardian Dentistry Partners",
            "North American Dental Group", "Affordable Dentures & Implants",
            "Dentures & Dental Services", "Northern Virginia Oral & Maxillofacial Surgery Associates",
            "monarch dental", "castle dental", "band & wire orthodontics"]

def word_count(all_brands):
    from collections import Counter
    from sqlalchemy import create_engine, text
    import re
    engine = create_engine("postgresql://postgres:postgres@199.241.139.206:5431/dental_jobs")
    sql = "select * from dso_scraping.dso_practices"
    exising_df = pd.read_sql(sql, con=engine)

    all_brands = [i.lower() for i in all_brands]

    all_brands = " ".join(all_brands)
    words = re.findall(r'\b\w+\b', all_brands.lower())

    # Create bigrams from the list of words
    bigrams = [' '.join(pair) for pair in zip(words, words[1:])]

    # Count the occurrences of each bigram
    bigram_counts = Counter(bigrams)
    print(bigram_counts)

all_brands = list(set(scraped_dso_databasea() + north_american_dental_group() + manual_adds()))
print(north_american_dental_group())