import os
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from utils.foreign_key_util import get_foreign_values

OUTPUT_DIR = "output"
DOMAIN = "employee"

row_num = 0
person_end_dates = {} 
person_start_dates = {}
_row_generator_cache = {}
_pk_cache = defaultdict(list)
_pk_generator = defaultdict(list)
_lookup_cache = {}

# === UTILS ===


def default(value):
    """Returns the value as is."""
    return value

# === PERSON_NUMBER ===


def generate_person_number():
    """
    Generate a synthetic PERSON_NUMBER value mimicking real-world patterns
    observed in your data sample.

    Patterns supported (with approximate probabilities based on your analysis):
      1. Letters followed by numbers (≈63%):
         - Examples: 'CA065870', 'USG54208', 'USX18000'
         - Common prefixes: 'USG', 'USX', 'USWL', 'CA', 'US', 'USWU'
         - The total length is typically 8 characters, rarely 9.
      2. Only numbers (≈36%):
         - Example: '50554793'
         - Usually starts with '50'
         - Total length is 8 digits

    Returns:
        str: A synthetic PERSON_NUMBER value similar to your source data.
    """
    # Choose the pattern based on observed frequency
    pattern = random.choices(
        ["letters_numbers", "only_numbers"],
        weights=[0.63, 0.36],  # Adjusted to match your data distribution
        k=1
    )[0]

    if pattern == "letters_numbers":
        # Common letter prefixes observed in your data
        letter_prefixes = [
            "USG", "USX", "USWL", "CA", "US", "USWU"
        ]
        prefix = random.choice(letter_prefixes)
        # Make total length 8 characters (majority of cases)
        num_length = 8 - len(prefix)
        # If prefix is longer, occasionally allow length 9 (rare in your data)
        if num_length < 3:
            num_length = 9 - len(prefix)
        digits = ''.join(random.choices("0123456789", k=num_length))
        return prefix + digits

    else:  # only_numbers
        # Most numeric IDs start with '50'
        prefix = "50"
        # Fill up to 8 digits
        digits = ''.join(random.choices("0123456789", k=6))
        return prefix + digits

# === DATES ===


def generate_dic(column_name, value):
    return {column_name: value}


def generate_end_date(person_id):

    columna_name = 'ENDDA'

    #CREATE ACTIVE EMPLOYEE
    if person_id not in person_start_dates:
        person_end_dates[person_id] = []
        person_end_dates[person_id].append('4712-12-31') 
        _row_generator_cache[row_num-1].update(generate_dic(columna_name, '4712-12-31'))
        return '4712-12-31'
    
    #CREATE INACTIVE EMPLOYEE
    else:
        date = datetime.strptime(person_start_dates[person_id][0], "%Y-%m-%d")
        date = date - timedelta(days=1)
        date = date.strftime("%Y-%m-%d")
        person_end_dates[person_id].append(date)
        _row_generator_cache[row_num-1].update(generate_dic(columna_name, date))
        return date
    

def generate_start_date(person_id, row_nums=None):
    columna_name = 'BEGDA'

    #CREATE ACTIVE EMPLOYEE
    if person_id not in person_start_dates:
        person_start_dates[person_id] = []
        start_date = random_date_between()
        person_start_dates[person_id].append(start_date) #active
        _row_generator_cache[row_num-1].update(generate_dic(columna_name, start_date))
        return start_date
    
    #CREATE INACTIVE EMPLOYEE
    else:
        end_date = datetime.strptime(person_end_dates[person_id][1], "%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date = random_date_between(max_date=end_date_str) #inactive
        _row_generator_cache[row_num-1].update(generate_dic(columna_name, start_date))
        return start_date


def random_date_between(min_date='2017-01-01',  max_date='2025-06-01', fmt='%Y-%m-%d'):

    """
    Genera una fecha aleatoria entre min_date y max_date.

    Args:
        min_date (str): Fecha mínima en formato string (ej. '2020-01-01').
        max_date (str): Fecha máxima en formato string (ej. '2023-12-31').
        fmt (str): Formato de las fechas (default '%Y-%m-%d').

    Returns:
        str: Fecha aleatoria como string en el formato especificado.
    """
    start = datetime.strptime(min_date, fmt)
    end = datetime.strptime(max_date, fmt)
    days_range = (end - start).days
    random_days = random.randint(0, days_range)
    random_date = start + timedelta(days=random_days)
    return random_date.strftime(fmt)

# === FOREING KEY AND COPY VALUES ===


def foreign_key(table_name, column_name):

    global row_num 

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


def copy_value_from_table(column_name, row_nums=None):
    """
    Devuelve el valor de una columna específica en una fila del dict.

    :param data: dict anidado (ej: {0: {'PERNR': '...', 'ENDDA': '...'}})
    :param row_num: int - número de fila (key externo)
    :param column_name: str - nombre de la columna a buscar
    :return: valor encontrado o None si no existe
    """
    try:
        return _row_generator_cache[row_nums][column_name]
    except KeyError as e:
        print(f"[⚠️ WARNING] {e} no encontrado en row {row_nums}")
        return None


def copy_value_from_column(source_value):

    """Returns the same value received (for passthrough rules)."""
    return source_value


def get_lookup_map(table_name, fk_column_name, domain="employee"):
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


def lookup_parent_value(table_name: str, fk_column_name, look_up_column, source_value, domain="employee"):
    """
    Usar un dict en memoria para acceso O(1).
    """
    lookup_map = get_lookup_map(table_name, fk_column_name, domain)
    row = lookup_map.get(source_value)
    if row is not None and look_up_column in row:
        return row[look_up_column]
    else:
        return None


# === COMPANY INFO ===


def get_company_code() -> str:
    """Returns a random Company function code."""
    return random.choice(["1710", "2910"])


def get_personnel_area(company_code):
    if company_code == '1710':
        return '1710'
    else:
        return '2910'
    

def get_personnel_subarea(company_code):
    if company_code == '1710':
        return '1710'
    else:
        return '2910'

def get_Organizational_Unit(company_code):
    if company_code == '1710':
        return '50000000'
    else:
        return '50000001'
    
# === PERSONNEL INFO ===


RESOURCE_PATH_SAP = os.path.join(os.path.dirname(__file__), "../resources/person_info.json")


with open(RESOURCE_PATH_SAP, "r") as f:
    SAP_PERSON_POOL = json.load(f)

PERSON_ID_LOOKUP = {person["PERSON_ID"]: person for person in SAP_PERSON_POOL}


def get_person_id():
    """Returns a random Person ID (Initials) from the SAP dummy data pool."""
    return random.choice(list(PERSON_ID_LOOKUP.keys()))


def get_inits(person_id):
    """Returns Last Name (NACHN) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("INITS", "")


def get_Last_Name(person_id):
    """Returns Last Name (NACHN) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("NACHN", "")


def get_Second_Name(person_id):
    """Returns Second Name (NACH2) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("NACH2", "")


def get_First_Name(person_id):
    """Returns First Name (VORNA) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("VORNA", "")


def get_Title(person_id):
    """Returns Title (TITEL) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("TITEL", "")


def get_Middle_Name(person_id):
    """Returns Middle Name (MIDNM) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("MIDNM", "")


def get_Gender(person_id):
    """Returns Gender (GESCH) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("GESCH", "")


def get_Date_of_Birth(person_id):
    """Returns Date of Birth (GBDAT) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("GBDAT", "")


def get_Nationality(person_id):
    """Returns Nationality (NATIO) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("NATIO", "")


def get_Communication_Language(table_name: str, fk_column_name, look_up_column, source_value):
    company_location = lookup_parent_value(table_name, fk_column_name, look_up_column, source_value)
    if company_location == '1710':
        return 'E'
    else:
        return random.choice(["E", "F"])
    

def get_Marital_Status_Key(person_id):
    """Returns Marital Status Key (FAMST) for a given Person ID."""
    return PERSON_ID_LOOKUP.get(person_id, {}).get("FAMST", "")


# === PERSONNEL INFO ===

def get_address_record_type() -> str:
    return random.choice(['01','02','03','04','05','06'])


RESOURCE_PATH_COM = os.path.join(os.path.dirname(__file__), "../resources/communication_records.json")


# === LOAD ADDRESS POOL ===
with open(RESOURCE_PATH_COM, "r") as f:
    COMMUNICATION_POOL = json.load(f)

# Suponiendo que ya cargaste COMMUNICATION_POOL y creaste el lookup:
COMMUNICATION_ID_LOOKUP = {comm["ID"]: comm for comm in COMMUNICATION_POOL}

def get_random_subty_spa0006() -> str:
    return random.choice(['1','2','3','4'])

def get_comm_id():
    """Returns a random Address ID from the address pool."""
    return random.choice(list(COMMUNICATION_ID_LOOKUP.keys()))


def get_Type_COM01(comm_id):
    """Returns Type COM01 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM01", "")


def get_Number_NUM01(comm_id):
    """Returns Number NUM01 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM01", "")


def get_Type_COM02(comm_id):
    """Returns Type COM02 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM02", "")


def get_Number_NUM02(comm_id):
    """Returns Number NUM02 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM02", "")


def get_Type_COM03(comm_id):
    """Returns Type COM03 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM03", "")


def get_Number_NUM03(comm_id):
    """Returns Number NUM03 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM03", "")


def get_Type_COM04(comm_id):
    """Returns Type COM04 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM04", "")


def get_Number_NUM04(comm_id):
    """Returns Number NUM04 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM04", "")


def get_Type_COM05(comm_id):
    """Returns Type COM05 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM05", "")


def get_Number_NUM05(comm_id):
    """Returns Number NUM05 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM05", "")


def get_Type_COM06(comm_id):
    """Returns Type COM06 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Type COM06", "")


def get_Number_NUM06(comm_id):
    """Returns Number NUM06 for a given ID."""
    return COMMUNICATION_ID_LOOKUP.get(comm_id, {}).get("Number NUM06", "")


# ===  INFOTYPE ===

def get_random_Infotype() -> str:
    """Returns a random code, only 'YNO1' currently."""
    return random.choice(['IT0000','IT0001','IT0002','IT0006','IT0105'])


def get_random_Subtype(INFTY):
    if INFTY == "IT0105":
        return '0010'
    elif INFTY == "IT0006":
        return '01'
    else:
        return None
    
# === HRP1001 ===

# OBJID (Empleado, OTYPE = 'P')


_OBJID_COUNTER = 0
_SOBID_COUNTER = 0


def generate_objid_p() -> str:
    """
    Genera valores OBJID secuenciales para empleados (OTYPE = 'P').
    Rango ejemplo: 10000001 - 19999999
    """
    global _OBJID_COUNTER
    start_range = 10_000_001
    end_range = 19_999_999
    objid = start_range + _OBJID_COUNTER
    if objid > end_range:
        raise ValueError("Exceeded OBJID (P) number range (10,000,001 - 19,999,999)")
    _OBJID_COUNTER += 1
    return str(objid)


def generate_sobid_s() -> str:
    """
    Genera valores SOBID secuenciales para posiciones (SCLAS = 'S').
    Rango ejemplo: 50000001 - 59999999
    """
    global _SOBID_COUNTER
    start_range = 50_000_001
    end_range = 59_999_999
    sobid = start_range + _SOBID_COUNTER
    if sobid > end_range:
        raise ValueError("Exceeded SOBID (S) number range (50,000,001 - 59,999,999)")
    _SOBID_COUNTER += 1
    return str(sobid)


# === EMPLOYEE ADDRESS DATA ===

RESOURCE_PATH = os.path.join(os.path.dirname(__file__), "../resources/employee_addresses_data.json")

# === LOAD ADDRESS POOL ===
with open(RESOURCE_PATH, "r") as f:
    ADDRESS_POOL = json.load(f)

# Creamos un lookup por Address ID (opcional si lo usas luego)
ADDRESS_ID_LOOKUP = {addr["Address ID"]: addr for addr in ADDRESS_POOL}


def get_address_id():
    """Returns a random Address ID from the address pool."""
    return random.choice(list(ADDRESS_ID_LOOKUP.keys()))

def get_post_stree_and_house_number(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Street and House Number", "")

def get_City(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("City", "")

def get_District(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("District", "")

def get_Postal_Code(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Postal Code", "")

def get_Country_Region_Key(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Country/Region Key", "")

def get_second_Address_Line(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("2nd Address Line", "")

def get_Street_two(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Street 2", "")

def get_Street_three(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Street 3", "")

def get_region(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Region (State, Province, County)", "")

def get_house_number(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("House Number", "")

def get_building_number(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Building (Number or Code)", "")

def get_floor_number(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Floor in Building", "")

def get_street_abbreviation(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Street Abbreviation", "")

def get_county_code(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("County Code", "")

def get_municipal_city_code(address_id):
    """Returns the postal code for a given street."""
    return ADDRESS_ID_LOOKUP.get(address_id, {}).get("Municipal City Code", "")