import os
import json
import random
from faker import Faker
from datetime import datetime

fake = Faker()

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

# --- OPTIMIZED LOOKUP FOR MATERIAL_NUMBER ---
MATERIAL_LOOKUP = {equip["Material Number"]: equip for equip in EQUIPMENT_POOL}

# === GENERATORS ===

def get_material_number():
    """Returns a random material number from the equipment pool."""
    return random.choice(list(MATERIAL_LOOKUP.keys()))

def get_equipment_description(material_number):
    """Returns description by material_number in O(1) time."""
    return MATERIAL_LOOKUP.get(material_number, {}).get("Description", "")

def get_equipment_weight(material_number):
    """Returns weight by material_number in O(1) time."""
    return MATERIAL_LOOKUP.get(material_number, {}).get("Weight", "")

def random_date_between(start_date: str, end_date: str):
    """Generates a random date between two dates (YYYY-MM-DD format)."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        return fake.date_between(start_date=start_dt, end_date=end_dt)
    except Exception as e:
        print(f"[❌ ERROR random_date_between] {e}")
        return None

def get_random_serial_number():
    """Generates a random serial number following one of several patterns."""
    patterns = [
        "???########",        # HQH826037
        "##???########",      # 08J120801117
        "########???",        # 460013341YP
        "###-???-########",   # 123-ABC-4567890
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern).upper()

def equipment_number():
    """Generates a random equipment number in different formats."""
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
