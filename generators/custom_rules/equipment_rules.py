import os
import json
import random
from faker import Faker
from datetime import datetime
from collections import defaultdict
from utils.foreign_key_util import get_foreign_values

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

material_number_set = set()

def get_material_number():
    """Returns a random material number from the equipment pool."""
    return generate_product_number()


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
        return fake.date_time_between(start_date=start_dt, end_date=end_dt).strftime("%Y-%m-%d %H:%M:%S")
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


equipment_number_set = set()

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
    while True:
        pattern = random.choice(patterns)
        equipment_number = fake.bothify(pattern).upper()
        if equipment_number not in equipment_number_set:
            equipment_number_set.add(equipment_number)
            return equipment_number


def generate_dic(column_name, value):
    return {column_name: value}


_pk_cache = defaultdict(list)     
_pk_generator = defaultdict(list)    # Trackea cuántas veces se ha usado cada valor
_row_generator_cache = {}


OUTPUT_DIR = "output"
DOMAIN = "equipment"  # o el que corresponda a ese archivo de reglas


def foreign_key(table_name, column_name, row_nums=None):
    row_num = row_nums

    key = f"{table_name}.{column_name}"

    if key not in _pk_cache:
        target_path = os.path.join(OUTPUT_DIR, DOMAIN, f"{table_name}.csv")
        _pk_cache[key] = get_foreign_values(target_path, column_name)

    if not _pk_cache[key]:
        print(f"[⚠️ WARNING] No hay valores en _pk_cache para {key}")
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

    # ♻️ Reset contador y vuelve a intentar
    print(f"[♻️ RESET] Reiniciando contador para {key}")
    keys_to_reset = [k for k in _pk_generator if k.startswith(f"{key}.")]
    for k in keys_to_reset:
        del _pk_generator[k]

    # Ahora elegir un nuevo valor limpio
    value = str(random.choice(_pk_cache[key]))
    _pk_generator[f"{key}.{value}"].append(value)
    _row_generator_cache[row_num] = generate_dic(column_name, value)
    row_num += 1  
    return value


fk_equipment_number_set = set()

def fk_copy():

        for equipment_id in equipment_number_set:
            if equipment_id not in fk_equipment_number_set:
                fk_equipment_number_set.add(equipment_id)
                return equipment_id
            

PATTERN_LETTERS = ['N', 'S', 'K', 'E', 'W', 'R', 'P', 'T', 'L']

product_number_set = set()


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
