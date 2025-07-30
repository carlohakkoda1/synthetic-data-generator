"""
Custom rule functions for synthetic product/material data generation.

Includes:
- Unique product number generation and assignment
- Product type/category logic
- Foreign key helpers and parent-child lookup
- Attribute generation (dimensions, weight, volume, etc.)
- Business rules (currency, valuation, tax, etc.)
- Utility for fetching random product descriptions
"""

import os
import random
import string
from datetime import datetime
import pandas as pd
import numpy as np
import json
from core.foreign_key_util import get_foreign_values

# === Constants and Globals ===

PATTERN_LETTERS = ['N', 'S', 'K', 'E', 'W', 'R', 'P', 'T', 'L']
product_number_set = set()
# Per-table unique tracking for foreign key usage
product_number_fk = {name: set() for name in [
    "smakt", "smarm", "smean", "smvke", "smlan", "smarc", "smard", 
    "smrparea", "smlgn", "smlgt", "smbew", "smbew_current", "smbew_future"
]}
product_number_dict = {}

# --- Product number and type generation ---

def generate_product_number(letter=None, store_in_dict=False):
    """
    Generates a unique product number with a pattern letter.
    Example: '123N54321'
    """
    while True:
        l = letter if letter else random.choice(PATTERN_LETTERS)
        if l not in PATTERN_LETTERS:
            raise ValueError(f"Invalid letter '{l}'. Must be one of {PATTERN_LETTERS}.")
        first_three = random.randint(0, 999)
        last_five = random.randint(0, 99999)
        product_number = f"{first_three:03d}{l}{last_five:05d}"
        if product_number not in product_number_set:
            product_number_set.add(product_number)
            if store_in_dict:
                product_number_dict[product_number] = {"letter": l}
            return product_number

def assign_product_type(product_number):
    """
    Assigns a product type (e.g. 'HAWA', 'FERT') based on the pattern letter.
    """
    letter_to_types = {
        'N': ['HAWA', 'FERT', 'DIEN'],
        'S': ['HAWA', 'FERT', 'DIEN', 'ERSA'],
        'K': ['HAWA', 'FERT', 'DIEN'],
        'E': ['HAWA', 'FERT'],
        'W': ['FERT'],
        'R': ['FERT'],
        'P': ['FERT'],
        'T': ['FERT'],
        'L': ['FERT'],
    }
    if len(product_number) != 9:
        raise ValueError("Invalid product number length. Expected 9 characters.")
    central_letter = product_number[3]
    types = letter_to_types.get(central_letter)
    if not types:
        raise ValueError(f"Unknown pattern letter: {central_letter}")
    return random.choice(types)

def old_product_id():
    """
    Generates a random 'old' product ID, with length and dash randomized.
    """
    length = random.choice([7, 8, 9, 10, 11])
    core = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    if random.random() < 0.5:
        dash_pos = random.randint(2, length - 2)
        core = core[:dash_pos] + '-' + core[dash_pos:]
    return core

# --- Date/time handling ---

_datetime_value = None
def get_datetime():
    """Returns a fixed datetime string for consistent data generation."""
    global _datetime_value
    if _datetime_value is None:
        _datetime_value = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    return _datetime_value

# --- Foreign key helpers (one-to-one relationship to S_* tables) ---

def fk_copy(table_name):
    """
    Returns a unique product number for use as a foreign key in a specific table.
    """
    tbl = table_name.lower()
    key = tbl.replace("s_", "") if tbl.startswith("s_") else tbl
    target_set = product_number_fk.get(key)
    if target_set is None:
        return ""
    for product_id in product_number_set:
        if product_id not in target_set:
            target_set.add(product_id)
            return product_id
    return ""

# --- Simple attribute/random assignment ---

def default_value(source_value):
    """Returns the input value unchanged (for passthrough)."""
    return source_value

def get_random_grouping_terms():
    """Returns a random grouping term."""
    return random.choice(["1", "2", "3", "4", "5"])

def get_random_delivery_plant():
    """Returns a random delivery plant code."""
    return random.choice(["CA32", "US32"])

def get_random_distribution_channels():
    """Returns a random distribution channel."""
    return random.choice(["10", "20", "30"])

def get_sales_org(delivery_plant):
    """Returns a sales org based on delivery plant."""
    if delivery_plant == 'CA32':
        return random.choice(["1704", "1710"])
    elif delivery_plant == 'US32':
        return random.choice(["2930", "2910"])
    else:
        return ""

# --- Parent/child lookups ---

_lookup_cache = {}

def get_lookup_map(table_name, fk_column_name, domain="material"):
    """
    Creates and caches a lookup: {fk_value: row_dict} for O(1) parent access.
    """
    key = f"{domain}.{table_name}.{fk_column_name}"
    if key not in _lookup_cache:
        path = os.path.join("output", domain, f"{table_name}.csv")
        if not os.path.exists(path):
            print(f"[⚠️ WARNING] {path} not found")
            _lookup_cache[key] = {}
            return _lookup_cache[key]
        df = pd.read_csv(path)
        # Take first appearance if duplicates exist
        _lookup_cache[key] = {row[fk_column_name]: row for _, row in df.iterrows()}
    return _lookup_cache[key]

def lookup_parent_value(table_name, fk_column_name, look_up_column, source_value, domain="material"):
    """
    Looks up a parent's value from a child record.
    """
    lookup_map = get_lookup_map(table_name, fk_column_name, domain)
    row = lookup_map.get(source_value)
    return row.get(look_up_column) if row and look_up_column in row else None

# --- Business rules ---

def get_country(table_name, fk_column_name, look_up_column, source_value):
    """Returns 'US' or 'CA' based on plant code from parent."""
    plant = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return {'US32': 'US', 'CA32': 'CA'}.get(plant, '')

def get_sales_tax_cat_one(country):
    """Returns sales tax category based on country."""
    return {'US': 'UTXJ', 'CA': 'CTXJ'}.get(country, '')

def get_valuation_class(table_name, fk_column_name, look_up_column, source_value):
    """Assigns valuation class based on parent product type and pattern letter."""
    product_type = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if product_type == 'HAWA':
        if len(source_value) >= 4 and source_value[3] == 'N':
            return '3100'
        else:
            return '3110'
    elif product_type == 'ERSA':
        return '3040'
    elif product_type == 'FERT':
        return '7920'
    elif product_type == 'DIEN':
        return '3300'
    return ''

def get_currency(table_name, fk_column_name, look_up_column, source_value):
    """Returns currency based on plant from parent."""
    plant = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    return {'US32': 'USD', 'CA32': 'CAD'}.get(plant, '')

def get_product_group(v_material_type):
    """Returns a product group based on material type."""
    if v_material_type in ('HAWA', 'FERT'):
        return random.choice([
            "43233410", "43212110", "44103101", "44103107", "44103108", "44103109", "44103004",
            "44103125", "44103110", "44101728", "44103121", "44122107",
            "44103127", "44103104", "44103120"
        ])
    elif v_material_type == 'DIEN':
        return '81112306'
    return ''

# --- Statistical attribute generation ---

def generate_gross_weight():
    """Returns a gross weight sampled from a log-normal distribution (clipped)."""
    mean = 0
    sigma = 1
    weight = np.random.lognormal(mean, sigma)
    return min(max(weight, 0.05), 50.0)

def generate_length():
    """Returns a sample length value."""
    return round(np.random.lognormal(2.5, 0.25), 2)

def generate_width():
    """Returns a sample width value."""
    return round(np.random.lognormal(2.0, 0.25), 2)

def generate_height():
    """Returns a sample height value."""
    return round(np.random.lognormal(1.7, 0.25), 2)

def generate_volume(length, width, height):
    """Returns the volume for the given dimensions."""
    return round(length * width * height, 2)

def generate_weight():
    """Returns a weight sampled from a log-normal distribution."""
    return round(np.random.lognormal(-0.2, 0.5), 2)

def copy_value_from_column(source_v):
    """Returns the same value received (for passthrough mapping)."""
    return source_v

def assign_random_mrp_controller():
    """Returns a random MRP controller code."""
    return random.choice([
        "U0V1", "6XCL", "EQ05", "C0Y1", "B1X0",
        "D2F1", "B2T0", "C0X1", "B1J1", "D2C1",
        "B1Y0", "1A1A", "C0U1", "B1F1", "A3R0"
    ])

def assign_country_origin(source_value):
    """Assigns country origin based on source value."""
    return 'US' if source_value == 'US32' else 'CA'

def generate_wzeit_replenishment_simple():
    """Returns a random replenishment time (days)."""
    return random.randint(7, 30)

def generate_plifz_simple():
    """Returns a random planned delivery time (days)."""
    return random.randint(7, 60)

def generate_webaz_simple():
    """Returns a random goods receipt processing time (days)."""
    return random.randint(2, 14)

# --- Product description lookup (from JSON pool) ---

RESOURCE_PATH_SAP = os.path.join(os.path.dirname(__file__), "../resources/product_descriptions.json")
with open(RESOURCE_PATH_SAP, "r") as f:
    SAP_PRODUCT_DESCRIPTION_POOL = json.load(f)

PRODUCT_DESCRIPTION_LOOKUP = {product["DESCRIPTION"]: product for product in SAP_PRODUCT_DESCRIPTION_POOL}

def get_product_description():
    """Returns a random product description from SAP dummy data pool."""
    return random.choice(list(PRODUCT_DESCRIPTION_LOOKUP.keys()))
