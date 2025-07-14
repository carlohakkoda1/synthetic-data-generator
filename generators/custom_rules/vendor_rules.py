import os
import json
import random
import re
import string
from faker import Faker
import pandas as pd

# === FAKER INSTANCES ===
fake = Faker()
fake_ca = Faker("en_CA")

# === CONSTANTS ===
_LIFNR_COUNTER = 0  # Global counter for LIFNRs (YN01)
RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/address_data.json")

# === LOAD ADDRESS POOL ===
with open(RESOURCE_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)

# === GENERATION FUNCTIONS ===

def supplier_code(name: str) -> str:
    """Generates a supplier code using the name and random digits."""
    prefix = ''.join(name.split())[:4].upper()
    suffix = str(random.randint(8000000000, 8999999999))
    return f"{prefix}-{suffix}"

def copy_value(source_value):
    """Returns the same value received (for passthrough rules)."""
    return source_value

def random_code() -> str:
    """Returns a random code, only 'YNO1' currently."""
    return random.choice(["YNO1"])

def get_random_partner_function() -> str:
    """Returns a random partner function code."""
    return random.choice(["RS", "WL", "LF", "BA"])

def default(value):
    """Returns the value as is."""
    return value

# === ADDRESS RELATED ===

def get_street():
    """Returns a random street from address pool."""
    address = random.choice(ADDRESS_POOL)
    return address["STREET"]

def get_post_code1(street):
    """Returns the postal code for a given street."""
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["POST_CODE1"]
    return ""

def get_city1(street):
    """Returns the city for a given street."""
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["CITY1"]
    return ""

def get_country(street):
    """Returns the country for a given street."""
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["COUNTRY"]
    return ""

def get_region(street):
    """Returns the region for a given street."""
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["REGION"]
    return ""

def get_langu_corr(street):
    """Returns the language code for a given street."""
    for addr in ADDRESS_POOL:
        if addr["STREET"] == street:
            return addr["LANGU_CORR"]
    return ""

# === SUPPLIER NAME-BASED ===

def supplier_id_from_name(name: str) -> str:
    """Generates a supplier ID from a name, with some randomness."""
    if not name:
        return ""
    clean_name = re.sub(r'[^A-Z0-9]', '', name.upper())
    words = re.findall(r'\b[A-Z0-9]', name.upper())
    prefix = ''.join(words)[:4] if len(words) >= 2 else clean_name[:4]
    suffix = (
        "-" + str(random.randint(1000000, 9999999))
        if random.random() < 0.5
        else str(random.randint(10, 99))
    )
    return f"{prefix}{suffix}"[:20]

# === PERSONA Y EMAIL ===

def optional_first_name():
    """Randomly returns a first name or None."""
    return fake.first_name() if random.random() < 0.8312 else None

def conditional_last_name(first_name):
    """Returns a last name if first name exists, else None."""
    return fake.last_name() if first_name else None

def phone_by_country(country):
    """Returns a random phone number for country, or None."""
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
    """Cleans a string for email use."""
    return re.sub(r'[^a-zA-Z0-9]', '', s or "").lower()

def email_from_name_company(company=None, first_name=None, last_name=None):
    """Generates email address based on company and name."""
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

# === LOOKUPS (PARENT/CHILD DATA) ===

def lookup_parent_value(table_name: str, fk_column_name, look_up_column, source_value):
    """Looks up a value in parent table CSV based on a FK."""
    path = os.path.join("output", "vendor", f"{table_name}.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        row = df[df[fk_column_name] == source_value]
        if not row.empty:
            value = row[look_up_column].iloc[0]
            return value
        else:
            print(f"[⚠️ WARNING] No match for value {source_value} in {table_name}")
            return None
    else:
        print(f"[⚠️ WARNING] File not found: {path}")
        return None

# === BUSINESS RULES (COMPANY, BANK, TAX, ETC) ===

def get_company_code(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns company code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 1704 if value == 'USA' else 2910

def get_purchasing_org(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns purchasing org code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return "US01" if value == 'USA' else "CAO1"

def get_bank_country(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns bank country code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return "US" if value == 'USA' else "CA"

def get_account_number(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns a BBAN based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return fake.bban() if value == 'USA' else fake_ca.bban()

def get_iban_number(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns an IBAN based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return fake.iban() if value == 'USA' else fake_ca.iban()

def get_currency(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns currency code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 'USD' if value == 'USA' else 'CAD'

def get_reconciliation_account(table_name: str, fk_column_name, look_up_column, source_value):
    """Returns GL account based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 21100000 if value == 'USA' else 21300000

def generate_bank_key(country_bank):
    """Returns a random BANKL (Bank Key) for USA or CANADA."""
    usa_bank_keys = ["021000021", "026009593", "121000248", "021000089", "091000022"]
    canada_bank_keys = ["0001004", "0040012", "0020020", "0010005", "0100063"]
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        return random.choice(usa_bank_keys)
    elif country_bank == "CA":
        return random.choice(canada_bank_keys)
    else:
        return None

def generate_bkont(country_bank):
    """Returns a random BKONT (Bank Control Key) for USA or CANADA."""
    usa_bkont_values = ["01", "02", "03"]  # Checking, Savings, Loan
    canada_bkont_values = ["01", "02", "03", "04", "05"]
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        return random.choice(usa_bkont_values)
    elif country_bank == "CA":
        return random.choice(canada_bkont_values)
    else:
        return None

def generate_bkref(country_bank):
    """Returns a random BKREF (Bank Reference) for USA or CANADA."""
    country_bank = country_bank.strip().upper()
    if country_bank == "US":
        return f"REF-{''.join(random.choices(string.digits, k=6))}"
    elif country_bank == "CA":
        return f"PAYID-{''.join(random.choices(string.digits, k=5))}"
    else:
        return None

def random_tax_type(ctx=None):
    """Randomly assigns a Tax Type (SAP field)."""
    return random.choice(['X', ''])

def generate_tax_number():
    """Returns a random tax number for USA (EIN) or CANADA (BN)."""
    country_code = random.choice(["US", "CA"]).strip().upper()
    if country_code == "US":
        # EIN: ##-#######
        return f"{''.join(random.choices('0123456789', k=2))}-{''.join(random.choices('0123456789', k=7))}"
    elif country_code == "CA":
        # BN: #########RT####
        base_number = ''.join(random.choices("0123456789", k=9))
        suffix = "RT" + ''.join(random.choices("0123456789", k=4))
        return f"{base_number}{suffix}"
    else:
        return None

def generate_lifnr_yn01() -> str:
    """Sequentially generates LIFNR values in range 300000000 - 399999999."""
    global _LIFNR_COUNTER
    start_range = 300_000_000
    end_range = 399_999_999
    lifnr = start_range + _LIFNR_COUNTER
    if lifnr > end_range:
        raise ValueError("Exceeded YN01 number range (300000000 - 399999999)")
    _LIFNR_COUNTER += 1
    return str(lifnr).zfill(9)
