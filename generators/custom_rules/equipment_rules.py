import os
import json
import random
from faker import Faker
from datetime import datetime

# Faker instance for generating synthetic data
fake = Faker()

# === UTILITIES ===

def default(value):
    """Returns the provided value as is."""
    return value

# === LOAD RESOURCES ===

RESOURCE_PATH = os.path.join(
    os.path.dirname(__file__), 
    "../resources/equipment_descriptions.json"
)

with open(RESOURCE_PATH, "r") as f:
    EQUIPMENT_POOL = json.load(f)

# === GENERATORS ===

def get_material_number():
    """
    Returns a random material number from the equipment pool.
    """
    equipment = random.choice(EQUIPMENT_POOL)
    return equipment["Material Number"]

def get_equipment_description(material_number):
    """
    Retrieves the equipment description for a given material number.
    """
    for equipment in EQUIPMENT_POOL:
        if equipment["Material Number"] == material_number:
            return equipment["Description"]
    return ""

def get_equipment_weight(material_number):
    """
    Retrieves the equipment weight for a given material number.
    """
    for equipment in EQUIPMENT_POOL:
        if equipment["Material Number"] == material_number:
            return equipment["Weight"]
    return ""

def random_date_between(start_date: str, end_date: str):
    """
    Generates a random date between two dates (YYYY-MM-DD format).
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        return fake.date_between(start_date=start_dt, end_date=end_dt)
    except Exception as e:
        print(f"[❌ ERROR random_date_between] {e}")
        return None

def get_random_serial_number():
    """
    Generates a random serial number following one of several patterns.
    """
    patterns = [
        "???########",        # HQH826037
        "##???########",      # 08J120801117
        "########???",        # 460013341YP
        "###-???-########",   # 123-ABC-4567890
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern).upper()

def equipment_number():
    """
    Generates a random equipment number in different formats.
    """
    patterns = [
        "???#####",         # EQ36198, A6330
        "#####-?",          # 48767-E
        "???#####-?",       # D-EQ114034
        "#########",        # 400207640
        "??#####-???",      # G-EQ7919
        "???##### (?????)"  # OH2936 (MTSE7)
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern).upper()
