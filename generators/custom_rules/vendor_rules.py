"""
Custom rules and utilities for synthetic vendor data generation.
Includes address handling, supplier/bank/tax code logic, parent-child lookups, and foreign key management.
"""

import os
import json
import random
import re
import string
from faker import Faker
import pandas as pd
from collections import defaultdict
from core.foreign_key_util import get_foreign_values

# === FAKER INSTANCES ===
fake = Faker()
fake_ca = Faker("en_CA")

# === CONSTANTS AND RESOURCES ===
_LIFNR_COUNTER = 0  # Global counter for LIFNRs (YN01)
RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/address_data.json")

with open(RESOURCE_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)

# Fast street/address lookups
STREET_LOOKUP = {addr["STREET"]: addr for addr in ADDRESS_POOL}

# --- CACHE for parent lookups ---
_lookup_cache = {}

# === SUPPLIER RULES ===

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
    return "YNO1"

def get_random_partner_function() -> str:
    """Returns a random partner function code."""
    return random.choice(["RS", "WL", "LF", "BA"])

def default(value):
    """Returns the value as is (default passthrough)."""
    return value

# === ADDRESS RULES ===

def get_street():
    """Returns a random street from address pool."""
    return random.choice(list(STREET_LOOKUP.keys()))

def get_post_code1(street):
    """Returns the postal code for a given street."""
    return STREET_LOOKUP.get(street, {}).get("POST_CODE1", "")

def get_city1(street):
    """Returns the city for a given street."""
    return STREET_LOOKUP.get(street, {}).get("CITY1", "")

def get_country(street):
    """Returns the country for a given street."""
    return STREET_LOOKUP.get(street, {}).get("COUNTRY", "")

def get_region(street):
    """Returns the region for a given street."""
    return STREET_LOOKUP.get(street, {}).get("REGION", "")

def get_langu_corr(street):
    """Returns the language code for a given street."""
    return STREET_LOOKUP.get(street, {}).get("LANGU_CORR", "")

# === SUPPLIER NAME-BASED ===

def supplier_id_from_name(name: str) -> str:
    """
    Generates a supplier ID from a name, with some randomness.
    """
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

# === PERSON AND EMAIL ===

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
    """Cleans a string for email use (alphanumeric, lowercased)."""
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

# === LOOKUPS (Parent/Child Data) ===

def get_lookup_map(table_name, fk_column_name, domain="vendor"):
    """
    Builds and caches a lookup: {fk_value: row_dict} for O(1) parent access.
    """
    key = f"{domain}.{table_name}.{fk_column_name}"
    if key not in _lookup_cache:
        path = os.path.join("output", domain, f"{table_name}.csv")
        if not os.path.exists(path):
            print(f"[⚠️ WARNING] {path} not found")
            _lookup_cache[key] = {}
            return _lookup_cache[key]
        df = pd.read_csv(path)
        # Always take the first appearance if duplicates
        _lookup_cache[key] = {row[fk_column_name]: row for _, row in df.iterrows()}
    return _lookup_cache[key]

def lookup_parent_value(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """
    O(1) access to a parent row's lookup value, using an in-memory dict.
    """
    lookup_map = get_lookup_map(table_name, fk_column_name, domain)
    row = lookup_map.get(source_value)
    if row is not None and look_up_column in row:
        return row[look_up_column]
    else:
        return None

# === BUSINESS RULES (Company, Bank, Tax, etc) ===

def get_company_code(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns the company code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return 1704 if value == 'USA' else 2910

def get_purchasing_org(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns the purchasing org code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return "US01" if value == 'USA' else "CAO1"

def get_bank_country(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns the bank country code based on parent value."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return "US" if value == 'USA' else "CA"

def get_account_number(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns a synthetic account number based on parent country."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return fake.bban() if value == 'USA' else fake_ca.bban()

def get_iban_number(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns a synthetic IBAN number based on parent country."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return fake.iban() if value == 'USA' else fake_ca.iban()

def get_currency(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns currency code based on parent country."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return 'USD' if value == 'USA' else 'CAD'

def get_reconciliation_account(table_name: str, fk_column_name, look_up_column, source_value, domain="vendor"):
    """Returns reconciliation account based on parent country."""
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain)
    return 21100000 if value == 'USA' else 21300000

def generate_bank_key(country_bank):
    """Returns a random bank key for USA or CANADA."""
    usa_bank_keys = ["021000021", "026009593", "121000248", "021000089", "091000022"]
    canada_bank_keys = ["0001004", "0040012", "0020020", "0010005", "0100063"]
    country_bank = (country_bank or "").strip().upper()
    if country_bank == "US":
        return random.choice(usa_bank_keys)
    elif country_bank == "CA":
        return random.choice(canada_bank_keys)
    else:
        return None

def generate_bkont(country_bank):
    """Returns a random bank control key for USA or CANADA."""
    usa_bkont_values = ["01", "02", "03"]  # Checking, Savings, Loan
    canada_bkont_values = ["01", "02", "03", "04", "05"]
    country_bank = (country_bank or "").strip().upper()
    if country_bank == "US":
        return random.choice(usa_bkont_values)
    elif country_bank == "CA":
        return random.choice(canada_bkont_values)
    else:
        return None

def generate_bkref(country_bank):
    """Returns a random bank reference for USA or CANADA."""
    country_bank = (country_bank or "").strip().upper()
    if country_bank == "US":
        return f"REF-{''.join(random.choices(string.digits, k=6))}"
    elif country_bank == "CA":
        return f"PAYID-{''.join(random.choices(string.digits, k=5))}"
    else:
        return None

def random_tax_type(ctx=None):
    """Randomly assigns a tax type (SAP field)."""
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

# === LIFNR (Vendor Number) and Foreign Key Handling ===

vendor_number_set = set()
vendor_number_fk_sddr_usage = set()

def generate_lifnr_yn01() -> str:
    """Sequentially generates LIFNR values in range 300000000 - 399999999."""
    global _LIFNR_COUNTER
    start_range = 300_000_000
    end_range = 399_999_999
    lifnr = start_range + _LIFNR_COUNTER
    if lifnr > end_range:
        raise ValueError("Exceeded YN01 number range (300000000 - 399999999)")
    _LIFNR_COUNTER += 1
    vendor_number = str(lifnr).zfill(9)
    if vendor_number not in vendor_number_set:
        vendor_number_set.add(vendor_number)
        return vendor_number
    return vendor_number

def fk_copy():
    """
    Returns a vendor number from vendor_number_set not yet used in vendor_number_fk_sddr_usage.
    """
    for vendor_id in vendor_number_set:
        if vendor_id not in vendor_number_fk_sddr_usage:
            vendor_number_fk_sddr_usage.add(vendor_id)
            return vendor_id

def generate_dic(column_name, value):
    """Utility: builds a dictionary with one column/value."""
    return {column_name: value}

# === Foreign Key Distribution (for test data relationships) ===

_pk_cache = defaultdict(list)
_pk_generator = defaultdict(list)    # Tracks how many times each value has been used
_row_generator_cache = {}

OUTPUT_DIR = "output"
DOMAIN = "vendor"  # Or as appropriate for this rule set

def foreign_key(table_name, column_name, row_nums=None):
    """
    Returns a foreign key value for use in generated test data, balancing usage.

    Args:
        table_name (str): The referenced table.
        column_name (str): The referenced column.
        row_nums (int, optional): The current row index.
    Returns:
        str: The chosen foreign key value.
    """
    row_num = row_nums
    key = f"{table_name}.{column_name}"

    if key not in _pk_cache:
        target_path = os.path.join(OUTPUT_DIR, DOMAIN, f"{table_name}.csv")
        _pk_cache[key] = get_foreign_values(target_path, column_name)

    if not _pk_cache[key]:
        print(f"[⚠️ WARNING] No values in _pk_cache for {key}")
        return ""

    attempts = 0
    max_attempts = len(_pk_cache[key]) * 2
    while attempts < max_attempts:
        value = str(random.choice(_pk_cache[key]))
        gen_key = f"{key}.{value}"
        count = len(_pk_generator[gen_key])
        if count < 2:
            _pk_generator[gen_key].append(value)
            _row_generator_cache[row_num] = generate_dic(column_name, value)
            row_num += 1 
            return value
        attempts += 1

    # Reset counter and try again
    keys_to_reset = [k for k in _pk_generator if k.startswith(f"{key}.")]
    for k in keys_to_reset:
        del _pk_generator[k]

    # Now choose a fresh value
    value = str(random.choice(_pk_cache[key]))
    _pk_generator[f"{key}.{value}"].append(value)
    _row_generator_cache[row_num] = generate_dic(column_name, value)
    row_num += 1  
    return value
