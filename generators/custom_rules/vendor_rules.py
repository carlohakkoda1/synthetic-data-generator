import os
import json
import random
import re
from faker import Faker
import pandas as pd


fake = Faker()
fake_ca = Faker("en_CA")


def supplier_code(name):
    prefix = ''.join(name.split())[:4].upper()
    suffix = str(random.randint(8000000000, 8999999999))
    return f"{prefix}-{suffix}"


def copy_value(source_value):
    return source_value


def random_code():
    return random.choice(["YNO1"])


def default(value):
    return value


RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/address_data.json")
with open(RESOURCE_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)


def get_street():
    address = random.choice(ADDRESS_POOL)
    return address["STREET"]


def get_post_code1(street):
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["POST_CODE1"]
    return ""


def get_city1(street):
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["CITY1"]
    return ""


def get_country(street):
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["COUNTRY"]
    return ""


def get_region(street):
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["REGION"]
    return ""


def get_langu_corr(street):
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["LANGU_CORR"]
    return ""


def supplier_id_from_name(name: str) -> str:
    if not name:
        return ""

    clean_name = re.sub(r'[^A-Z0-9]', '', name.upper())

    words = re.findall(r'\b[A-Z0-9]', name.upper())
    prefix = ''.join(words)[:4] if len(words) >= 2 else clean_name[:4]

    if random.random() < 0.5:
        suffix = "-" + str(random.randint(1000000, 9999999))
    else:
        suffix = str(random.randint(10, 99))

    return f"{prefix}{suffix}"[:20]


def optional_first_name():
    return fake.first_name() if random.random() < 0.8312 else None


def conditional_last_name(first_name):
    return fake.last_name() if first_name else None


def phone_by_country(country):
    if random.random() < 0.2924:
        return None

    country = str(country).strip().upper()

    if country == "USA":
        return fake.phone_number()
    elif country == "CANADA":
        return fake_ca.phone_number()
    else:
        return None
    
def clean_string(s):
    return re.sub(r'[^a-zA-Z0-9]', '', s or "").lower()

def email_from_name_company(company=None, first_name=None, last_name=None):
    if random.random() < 0.746:
        return None

    company = clean_string(company)
    first_name = clean_string(first_name)
    last_name = clean_string(last_name)

    if first_name and last_name:
        user_part = f"{first_name}.{last_name}"
    elif first_name:
        user_part = first_name
    elif last_name:
        user_part = last_name
    else:
        user_part = fake.user_name()

    domain_part = f"{company}.com" if company else fake.free_email_domain()

    return f"{user_part}@{domain_part}"


def lookup_parent_value(table_name: str, fk_column_name, look_up_column, source_value):

    path = os.path.join("output", "vendor", f"{table_name}.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        row = df[df[fk_column_name] == source_value] 
        value = row[look_up_column].iloc[0]
        return value 
        
    else:
        print(f"[⚠️ WARNING] {path}")
        return None


def get_company_code(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return 1704
    else:
        return 2910


def get_purchasing_org(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return "US01"
    else:
        return "CAO1"