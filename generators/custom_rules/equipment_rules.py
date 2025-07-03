import os
import json
import random
from faker import Faker
from datetime import datetime



fake = Faker()

def default(value):
    return value



RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/equipment_descriptions.json")
with open(RESOURCE_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)


def get_material_number():
    address = random.choice(ADDRESS_POOL)
    return address["Material Number"]


def get_equipment_description(material_number):
    for addr in ADDRESS_POOL:
        if addr["Material Number"] == material_number:
            return addr["Description"]
    return ""


def get_equipment_weight(material_number):
    for addr in ADDRESS_POOL:
        if addr["Material Number"] == material_number:
            return addr["Weight"]
    return ""


def random_date_between(start_date: str, end_date: str):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        return fake.date_between(start_date=start_dt, end_date=end_dt)
    except Exception as e:
        print(f"[❌ ERROR random_date_between] {e}")
        return None
    

def get_random_serial_number():
    patterns = [
        "???########",       # HQH826037
        "##???########",     # 08J120801117
        "########???",       # 460013341YP
        "###-???-########",  # 123-ABC-4567890
    ]
    pattern = random.choice(patterns)
    return fake.bothify(pattern)


def equipment_number():
    pattern = random.choice([
        "???#####",        # EQ36198, A6330
        "#####-?",         # 48767-E
        "???#####-?",      # D-EQ114034
        "#########",       # 400207640
        "??#####-???",     # G-EQ7919
        "???##### (?????)" # OH2936 (MTSE7)
    ])
    return fake.bothify(pattern).upper()

