import os
import json
import random
import re
from faker import Faker
import pandas as pd
import string


fake = Faker()
fake_ca = Faker("en_CA")
_lifnr_counter = 0  # Global counter to keep track of generated LIFNRs for YN01



def supplier_code(name):
    prefix = ''.join(name.split())[:4].upper()
    suffix = str(random.randint(8000000000, 8999999999))
    return f"{prefix}-{suffix}"


def copy_value(source_value):
    return source_value


def random_code():
    return random.choice(["YNO1"])


def get_random_partner_function():
    return random.choice(["RS", "WL", "LF", "BA"])


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
    

def get_bank_country(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return "US"
    else:
        return "CA"
    
def get_account_number(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return fake.bban()
    else:
        return fake_ca.bban()
    
def get_iban_number(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return fake.iban()
    else:
        return fake_ca.iban()
    
def get_currency(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return 'USD'
    else:
        return 'CAD'


def get_reconciliation_account(table_name: str, fk_column_name, look_up_column, source_value):

    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if value == 'USA':
        return 21100000
    else:
        return 21300000
    

def generate_bank_key(country_bank):
    """
    Returns a random BANKL (Bank Key) for USA or CANADA.
    """
    usa_bank_keys = [
        "021000021", "026009593", "121000248", "021000089", "091000022"
    ]
    canada_bank_keys = [
        "0001004", "0040012", "0020020", "0010005", "0100063"
    ]
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        return random.choice(usa_bank_keys)
    elif country_bank == "CA":
        return random.choice(canada_bank_keys)
    else:
        print(f"[⚠️ WARNING] {path}")
        return None
    
def generate_bkont(country_bank):
    """
    Returns a random BKONT (Bank Control Key) for USA or CANADA.
    """
    usa_bkont_values = ["01", "02", "03"]  # Checking, Savings, Loan
    canada_bkont_values = ["01", "02", "03", "04", "05"]
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        return random.choice(usa_bkont_values)
    elif country_bank == "CA":
        return random.choice(canada_bkont_values)
    else:
        return 2910

def generate_bkref(country_bank):
    """
    Returns a random BKREF (Bank Reference) for USA or CANADA.
    """
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        prefix = "REF-"
        random_part = ''.join(random.choices(string.digits, k=6))
        return f"{prefix}{random_part}"
    elif country_bank == "CA":
        prefix = "PAYID-"
        random_part = ''.join(random.choices(string.digits, k=5))
        return f"{prefix}{random_part}"
    else:
        return "CAO1"
    

def random_tax_type(ctx=None):
    """Randomly assigns a Tax Type (SAP field)."""
    return random.choice(['X', ''])

def generate_tax_number():
    """
    Returns a random tax number for USA (EIN) or CANADA (BN).
    """
    country_code = random.choice(["US", "CA"])
    country_code = country_code.strip().upper()
    if country_code == "US":
        # EIN: ##-####### (e.g., 82-3894710)
        first_part = ''.join(random.choices("0123456789", k=2))
        second_part = ''.join(random.choices("0123456789", k=7))
        return f"{first_part}-{second_part}"
    elif country_code == "CA":
        # BN: #########RT#### (e.g., 342601701RT8476)
        base_number = ''.join(random.choices("0123456789", k=9))
        suffix = "RT" + ''.join(random.choices("0123456789", k=4))
        return f"{base_number}{suffix}"
    else:
        return "CA"


def generate_lifnr_yn01() -> int:
    global _lifnr_counter
    start_range = 300000000
    end_range = 399999999

    lifnr = start_range + _lifnr_counter
    if lifnr > end_range:
        raise ValueError("Exceeded YN01 number range (300000000 - 399999999)")

    _lifnr_counter += 1  # Increment for the next call
    result = str(lifnr).zfill(9)
    return result