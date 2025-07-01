"""
Module for loading and sampling synthetic location data
from a predefined JSON file located in 'resources/locations.json'.
"""

import json
import random


# Load location data from file
with open("resources/locations.json", encoding="utf-8") as f:
    LOCATIONS = json.load(f)


def sample_random_location():
    """
    Randomly selects and returns a location dictionary for
    either USA or Canada.

    The function randomly chooses a country (USA or CANADA), filters 
    the locations to match valid country codes, and returns one random result.
    
    Returns:
        dict: A randomly selected location with keys like
        COUNTRY, CITY1, REGION, etc.
    """
    country = random.choice(["USA", "CANADA"])
    country_codes = ["USA", "UNITED STATES", "US"] if country == "USA" else ["CA", "CANADA"]

    pool = [loc for loc in LOCATIONS if loc["COUNTRY"].upper() in country_codes]
    return random.choice(pool)
