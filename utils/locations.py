import json
import random

with open("resources/locations.json") as f:
    LOCATIONS = json.load(f)


def sample_random_location():
    country = random.choice(["USA", "CANADA"])
    country_codes = ["USA", "UNITED STATES", "US"] if country == "USA" else ["CA", "CANADA"]
    pool = [loc for loc in LOCATIONS if loc["COUNTRY"].upper() in country_codes]
    return random.choice(pool)
