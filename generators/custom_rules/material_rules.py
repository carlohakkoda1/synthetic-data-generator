import random
import string
import os
from datetime import datetime
import pandas as pd
import numpy as np
import json
from core.foreign_key_util import get_foreign_values


PATTERN_LETTERS = ['N', 'S', 'K', 'E', 'W', 'R', 'P', 'T', 'L']

# Set for fast uniqueness checking
product_number_set = set()
product_number_fk_smakt = set()
product_number_fk_smarm = set()
product_number_fk_smean = set()
product_number_fk_smvke = set()
product_number_fk_smlan = set()
product_number_fk_smarc = set()
product_number_fk_smard = set()
product_number_fk_smrparea = set()
product_number_fk_smlgn = set()
product_number_fk_smlgt = set()
product_number_fk_smbew = set()
product_number_fk_smbew_current = set()
product_number_fk_smbew_future = set()


# Dict if you want to store more details (optional)
product_number_dict = dict()


def default_value(source_value):
    return source_value

def generate_product_number(letter=None, store_in_dict=False):
    """
    Generates a unique product number and stores it in a set/dict for uniqueness validation.
    If the generated product number already exists, keep generating until unique.

    :param letter: Optional; choose from PATTERN_LETTERS or random.
    :param store_in_dict: If True, store in dict with extra info; otherwise, just in set.
    :return: Unique product number string.
    """
    while True:
        if letter is None:
            l = random.choice(PATTERN_LETTERS)
        else:
            l = letter
            if l not in PATTERN_LETTERS:
                raise ValueError(f"Invalid letter '{l}'. Must be one of {PATTERN_LETTERS}.")

        first_three = random.randint(0, 999)
        last_five = random.randint(0, 99999)
        product_number = f"{first_three:03d}{l}{last_five:05d}"

        if product_number not in product_number_set:
            product_number_set.add(product_number)
            return product_number
        # Else, loop again to get a new product number

def assign_product_type(product_number):
    """
    Assigns a single, random product type to the given product number based on its pattern letter.
    :param product_number: String, e.g., '123N54321'
    :return: Assigned product type (string)
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
    length = random.choice([7, 8, 9, 10, 11])
    core = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    if random.random() < 0.5:  # 50% chance to insert a dash
        dash_pos = random.randint(2, length-2)
        core = core[:dash_pos] + '-' + core[dash_pos:]
    return core

_datetime_value = None

def get_datetime():
    global _datetime_value
    if _datetime_value is None:
        _datetime_value = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    return _datetime_value



def fk_copy(table_name):

    if table_name == 'S_MAKT':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smakt:
                product_number_fk_smakt.add(product_id)
                return product_id
    elif table_name == 'S_MARM':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smarm:
                product_number_fk_smarm.add(product_id)
                return product_id
    elif table_name == 'S_MEAN':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smean:
                product_number_fk_smean.add(product_id)
                return product_id
    elif table_name == 'S_MVKE':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smvke:
                product_number_fk_smvke.add(product_id)
                return product_id
    elif table_name == 'S_MLAN':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smlan:
                product_number_fk_smlan.add(product_id)
                return product_id
    elif table_name == 'S_MARC':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smarc:
                product_number_fk_smarc.add(product_id)
                return product_id
    elif table_name == 'S_MARD':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smard:
                product_number_fk_smard.add(product_id)
                return product_id
    elif table_name == 'S_MRP_AREA':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smrparea:
                product_number_fk_smrparea.add(product_id)
                return product_id
    elif table_name == 'S_MLGN':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smlgn:
                product_number_fk_smlgn.add(product_id)
                return product_id            
    elif table_name == 'S_MLGT':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smlgt:
                product_number_fk_smlgt.add(product_id)
                return product_id    
    elif table_name == 'S_MBEW':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smbew:
                product_number_fk_smbew.add(product_id)
                return product_id
    elif table_name == 'S_MBEW_CURRENT':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smbew_current:
                product_number_fk_smbew_current.add(product_id)
                return product_id            
    elif table_name == 'S_MBEW_FUTURE':
        for product_id in product_number_set:
            if product_id not in product_number_fk_smbew_future:
                product_number_fk_smbew_future.add(product_id)
                return product_id
    else:
        return ""   

def get_random_grouping_terms():
    return random.choice(["1", "2", "3", "4", "5"])     

def get_random_delivery_plant():
    return random.choice(["CA32", "US32"])

def get_random_distribution_channels():
    return random.choice(["10", "20", "30"])

def get_sales_org(delivery_plant):
    if delivery_plant == 'CA32':
        return random.choice(["1704", "1710"])
    elif delivery_plant == 'US32':
        return random.choice(["2930", "2910"])
    else:
        return ""
    
_lookup_cache = {}

def lookup_parent_value(table_name: str, fk_column_name, look_up_column, source_value, domain="material"):
    """
    Usar un dict en memoria para acceso O(1).
    """
    lookup_map = get_lookup_map(table_name, fk_column_name, domain)
    row = lookup_map.get(source_value)
    if row is not None and look_up_column in row:
        return row[look_up_column]
    else:
        return None
    
def get_lookup_map(table_name, fk_column_name, domain="material"):
    """
    Crea y cachea un dict: {fk_value: row_dict} para accesso O(1).
    """
    key = f"{domain}.{table_name}.{fk_column_name}"
    if key not in _lookup_cache:
        path = os.path.join("output", domain, f"{table_name}.csv")
        if not os.path.exists(path):
            print(f"[⚠️ WARNING] {path} not found")
            _lookup_cache[key] = {}
            return _lookup_cache[key]
        df = pd.read_csv(path)
        # OJO: Puede haber duplicados, siempre toma la PRIMERA aparición
        _lookup_cache[key] = {row[fk_column_name]: row for _, row in df.iterrows()}
    return _lookup_cache[key]


def get_country(table_name: str, fk_column_name, look_up_column, source_value):
    plant = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if plant == 'US32':
        return 'US'
    elif plant == 'CA32':
        return 'CA'
    else:
        return ''


def get_sales_tax_cat_one(country):
    if country == 'US':
        return 'UTXJ'
    elif country == 'CA':
        return 'CTXJ'
    else:
        return ''

def get_valuation_class(table_name: str, fk_column_name, look_up_column, source_value):
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
    else:
        return ''


def get_currency(table_name: str, fk_column_name, look_up_column, source_value):
    plant = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if plant == 'US32':
        return 'USD'
    elif plant == 'CA32':
        return 'CAD'
    else:
        return ''


def get_product_group(v_material_type):
    if v_material_type == 'HAWA' or v_material_type == 'FERT':
        return random.choice(["43233410", "43212110", "44103101", "44103107",
                            "44103108", "44103109", "44103004",
                            "44103125", "44103110", "44101728", "44103121", "44122107",
                            "44103127", "44103104", "44103120"])
    elif v_material_type == 'DIEN':
        return '81112306'
    else:
        return ''

def generate_gross_weight():
    # Adjust mean and sigma to get a similar shape
    mean = 0  # Try with 0, tune as needed
    sigma = 1  # Start with 1, lower for tighter spread, higher for more spread
    weight = np.random.lognormal(mean, sigma)
    # Optional: Clip to your observed min/max range to avoid crazy outliers
    return min(max(weight, 0.05), 50.0)


def copy_value_from_column(source_v):
    return source_v


def generate_length():
    return round(np.random.lognormal(2.5, 0.25), 2)

def generate_width():
    return round(np.random.lognormal(2.0, 0.25), 2)

def generate_height():
    return round(np.random.lognormal(1.7, 0.25), 2)

def generate_volume(length, width, height):
    return round(length * width * height, 2)

def generate_weight():
    # Example: lognormal parameters for weight; tune as needed
    return round(np.random.lognormal(-0.2, 0.5), 2)

def assign_random_mrp_controller():
    return random.choice([
    "U0V1", "6XCL", "EQ05", "C0Y1", "B1X0",
    "D2F1", "B2T0", "C0X1", "B1J1", "D2C1",
    "B1Y0", "1A1A", "C0U1", "B1F1", "A3R0"
    ])


def assing_country_origin(source_value):
    if source_value == 'US32':
        return 'US'
    else:
        return 'CA'


def generate_wzeit_replenishment_simple():
    return random.randint(7, 30)

def generate_plifz_simple():
    return random.randint(7, 60)

def generate_webaz_simple():
    return random.randint(2, 14)



RESOURCE_PATH_SAP = os.path.join(os.path.dirname(__file__), "../resources/product_descriptions.json")

with open(RESOURCE_PATH_SAP, "r") as f:
    SAP_PRODUCT_DESCRIPTION_POOL = json.load(f)

PRODUCT_DESCRIPTION_LOOKUP = {product["DESCRIPTION"]: product for product in SAP_PRODUCT_DESCRIPTION_POOL}


def get_product_description():
    """Returns a random Person ID (Initials) from the SAP dummy data pool."""
    return random.choice(list(PRODUCT_DESCRIPTION_LOOKUP.keys()))