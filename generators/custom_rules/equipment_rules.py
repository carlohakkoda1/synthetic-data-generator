"""
equipment_rules.py
------------------
Helpers for generating synthetic equipment/material data,
based on a static resource JSON file.

Functions:
    - get_material_number()
    - get_equipment_description(material_number)
    - get_equipment_weight(material_number)
    - random_date_between(start_date, end_date)
    - get_random_serial_number()
    - equipment_number()
    - default(value)
"""

import os
import json
import random
from faker import Faker
from datetime import datetime

fake = Faker()

# ---------------------------------------------------------------------------
# Load Equipment Resource Pool
# ---------------------------------------------------------------------------
RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/equipment_descriptions.json")
with open(RESOURCE_PATH, "r") as f:
    EQUIPMENT_POOL = json.load(f)

# ---------------------------------------------------------------------------
# Basic Helpers
# ---------------------------------------------------------------------------


def default(value):
    """Pass-through: returns the provided value unchanged."""
    return value

# ---------------------------------------------------------------------------
# Equipment Data Generators
# ---------------------------------------------------------------------------


def get_material_number():
    """
    Returns a random material number from the equipment pool.
    """
    entry = random.choice(EQUIPMENT_POOL)
    return entry["Material Number"]


def get_equipment_description(material_number):
    """
    Looks up the description for a given material number.
    Returns an empty string if not found.
    """
    for entry in EQUIPMENT_POOL:
        if entry["Material Number"] == material_number:
            return entry["Description"]
    return ""


def get_equipment_weight(material_number):
    """
    Looks up the weight for a given material number.
    Returns an empty string if not found.
    """
    for entry in EQUIPMENT_POOL:
        if entry["Material Number"] == material_number:
            return entry["Weight"]
    return ""


def random_date_between(start_date: str, end_date: str):
    """
    Returns a random date between start_date and end_date (YYYY-MM-DD).
    Returns None if parsing fails.
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
    Returns a random equipment serial number following one of several patterns.
    Example outputs: HQH826037, 08J120801117, 460013341YP, 123-ABC-4567890
    """
    patterns = [
        "???########",       # HQH826037
        "##???########",     # 08J120801117
        "########???",       # 460013341YP
        "###-???-########",  # 123-ABC-4567890
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern).upper()


def equipment_number():
    """
    Returns a random equipment number following one of several formats.
    Example outputs: EQ36198, 48767-E, D-EQ114034, 400207640, G-EQ7919, OH2936 (MTSE7)
    """
    patterns = [
        "???#####",        # EQ36198, A6330
        "#####-?",         # 48767-E
        "???#####-?",      # D-EQ114034
        "#########",       # 400207640
        "??#####-???",     # G-EQ7919
        "???##### (?????)" # OH2936 (MTSE7)
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern).upper()
