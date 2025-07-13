"""

vendor_rules.py
---------------
Utility functions for generating synthetic vendor data,
address fields, banking info, tax IDs, and other related values
for S/4Hanna synthetic data.

Includes parent table caching, address pool management,
ID and code generation, and several SAP domain-specific helpers.

Requires:
    - resources/address_data.json
    - pandas, faker

Main Groups:
    - Parent table lookup & cache
    - Supplier, code, and address helpers
    - Company/banking value generators
    - Contact info generators
    - SAP domain helpers (bank keys, taxes, etc)
    - Utility/pass-throughs
"""

import os
import json
import random
import re
import string
from faker import Faker
import pandas as pd

# ---------------------------------------------------------------------------
# Globals and Resource Pools
# ---------------------------------------------------------------------------
fake = Faker()
fake_ca = Faker("en_CA")
_lifnr_counter = 0  # Global counter to keep track of generated LIFNRs for YN01



_PARENT_TABLE_CACHE = {}
_PARENT_LOOKUP_DICT_CACHE = {}

ADDRESS_PATH = os.path.join(os.path.dirname(__file__), "../resources/address_data.json")
with open(ADDRESS_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)

# ---------------------------------------------------------------------------
# Parent Table Lookup & Cache
# ---------------------------------------------------------------------------
def lookup_parent_value(table_name: str, fk_column_name: str, look_up_column: str, source_value):
    """
    Efficiently looks up a value in a parent table by foreign key.
    Uses in-memory caching for performance.
    """
    path = os.path.join("output", "vendor", f"{table_name}.csv")
    cache_key = (path, fk_column_name, look_up_column)

    # Cache DataFrame
    if path not in _PARENT_TABLE_CACHE:
        if os.path.exists(path):
            _PARENT_TABLE_CACHE[path] = pd.read_csv(path)
        else:
            print(f"[⚠️ WARNING] Parent table not found: {path}")
            return None
    df = _PARENT_TABLE_CACHE[path]

    # Cache lookup dictionary
    if cache_key not in _PARENT_LOOKUP_DICT_CACHE:
        _PARENT_LOOKUP_DICT_CACHE[cache_key] = df.set_index(fk_column_name)[look_up_column].to_dict()
    lookup_dict = _PARENT_LOOKUP_DICT_CACHE[cache_key]

    return lookup_dict.get(source_value, None)

def clear_caches():
    """
    Clears all in-memory parent table and lookup caches.
    Useful for unit testing or reloading sources.
    """
    _PARENT_TABLE_CACHE.clear()
    _PARENT_LOOKUP_DICT_CACHE.clear()

# ---------------------------------------------------------------------------
# Supplier & ID Generation
# ---------------------------------------------------------------------------


def generate_lifnr_yn01() -> str:
    """
    Generates a unique synthetic Vendor Number (LIFNR) for BP Grouping YN01.

    📌 Description:
    - Simulates SAP S/4HANA number range assignment for vendors in YN01.
    - Each call produces a sequential 9-digit number within 300000000-399999999.

    🚨 Notes:
    - Maintains a global counter to increment numbers for each row generated.
    - Raises an error if the range is exceeded.

    🔥 Returns:
    -------
    str
        A 9-digit zero-padded Vendor Number (LIFNR).

    📝 Example:
    --------
    >>> generate_lifnr_yn01()
    '300000000'

    >>> generate_lifnr_yn01()
    '300000001'
    """
    global _lifnr_counter
    start_range = 300000000
    end_range = 399999999

    lifnr = start_range + _lifnr_counter
    if lifnr > end_range:
        raise ValueError("Exceeded YN01 number range (300000000 - 399999999)")

    _lifnr_counter += 1  # Increment for the next call
    return str(lifnr).zfill(9)


def supplier_id_from_name(name: str) -> str:
    """
    Generates a synthetic supplier ID from a supplier name.
    """
    if not name:
        return ""
    clean_name = re.sub(r'[^A-Z0-9]', '', name.upper())
    words = re.findall(r'\b[A-Z0-9]', name.upper())
    prefix = ''.join(words)[:4] if len(words) >= 2 else clean_name[:4]
    suffix = ("-" + str(random.randint(1000000, 9999999))) if random.random() < 0.5 else str(random.randint(10, 99))
    return f"{prefix}{suffix}"[:20]

def supplier_id_from_table(table_name: str, fk_column_name: str, look_up_column: str, source_value):
    """
    Generates a supplier ID from a value looked up in a parent table.
    """
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if not value:
        return ""
    clean_name = re.sub(r'[^A-Z0-9]', '', value.upper())
    words = re.findall(r'\b[A-Z0-9]', value.upper())
    prefix = ''.join(words)[:4] if len(words) >= 2 else clean_name[:4]
    suffix = ("-" + str(random.randint(1000000, 9999999))) if random.random() < 0.5 else str(random.randint(10, 99))
    return f"{prefix}{suffix}"[:20]

def supplier_code(name: str) -> str:
    """
    Generates a pseudo-random supplier code based on the name.
    """
    prefix = ''.join(name.split())[:4].upper()
    suffix = str(random.randint(8000000000, 8999999999))
    return f"{prefix}-{suffix}"

# ---------------------------------------------------------------------------
# Address Pool Value Helpers (Synthetic, Consistent by STREET)
# ---------------------------------------------------------------------------
def get_street():
    """Returns a random street from the address pool."""
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

# ---------------------------------------------------------------------------
# Company, Purchasing Org, Bank, Account Number, IBAN, Currency, Reconciliation
# ---------------------------------------------------------------------------
def get_company_code(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 1704 if value == 'USA' else 2910

def get_purchasing_org(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return "US01" if value == 'USA' else "CAO1"

def get_bank_country(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return "US" if value == 'USA' else "CA"

def get_account_number(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return fake.bban() if value == 'USA' else fake_ca.bban()

def get_iban_number(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return fake.iban() if value == 'USA' else fake_ca.iban()

def get_currency(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 'USD' if value == 'USA' else 'CAD'

def get_reconciliation_account(table_name, fk_column_name, look_up_column, source_value):
    value = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return 21100000 if value == 'USA' else 21300000

# ---------------------------------------------------------------------------
# Contact and Email Generators
# ---------------------------------------------------------------------------
def optional_first_name():
    """
    Returns a random first name or None (weighted).
    """
    return fake.first_name() if random.random() < 0.8312 else None

def conditional_last_name(first_name):
    """
    Returns a random last name if first_name is not None, else None.
    """
    return fake.last_name() if first_name else None

def phone_by_country(country):
    """
    Returns a synthetic phone number for the given country, or None (randomly).
    """
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
    """Returns a lowercased string with only alphanumerics."""
    return re.sub(r'[^a-zA-Z0-9]', '', s or "").lower()

def email_from_name_company(company=None, first_name=None, last_name=None):
    """
    Generates an email based on company and name, or returns None (weighted).
    """
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

# ---------------------------------------------------------------------------
# SAP Bank Key/Control/Reference Generators
# ---------------------------------------------------------------------------
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
        raise ValueError("Invalid country. Expected 'USA' or 'CANADA'.")

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
        raise ValueError("Invalid country. Expected 'USA' or 'CANADA'.")

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
        prefix = "BKREF"
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
        return f"{prefix}{random_part}"

# ---------------------------------------------------------------------------
# Miscellaneous SAP Value Generators
# ---------------------------------------------------------------------------
def random_partner_function(ctx=None):
    """Randomly assigns a Partner Function."""
    return random.choice(['LF', 'BA', 'RS', 'ZB'])

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
        raise ValueError("Invalid country. Expected 'USA' or 'CANADA'.")

# ---------------------------------------------------------------------------
# Utility/Pass-through
# ---------------------------------------------------------------------------
def copy_value(source_value):
    """Returns the source value unchanged."""
    return source_value

def random_code():
    """Returns a fixed example code (adapt as needed)."""
    return random.choice(["YNO1"])

def default(value):
    """Returns the value unchanged (default passthrough)."""
    return value
