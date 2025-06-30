import os
import json
import random
import re
from faker import Faker


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
    """Retorna un STREET aleatorio y funciona como valor raíz."""
    address = random.choice(ADDRESS_POOL)
    return address["STREET"]


def get_post_code1(street):
    """Retorna el POST_CODE1 correspondiente al STREET"""
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

    # Limpiar nombre y quitar caracteres no alfanuméricos
    clean_name = re.sub(r'[^A-Z0-9]', '', name.upper())

    # Prefijo: tomar primeras 3-4 letras o iniciales
    words = re.findall(r'\b[A-Z0-9]', name.upper())
    prefix = ''.join(words)[:4] if len(words) >= 2 else clean_name[:4]

    # Sufijo: a veces se usa número, a veces no
    if random.random() < 0.5:
        suffix = "-" + str(random.randint(1000000, 9999999))
    else:
        suffix = str(random.randint(10, 99))

    return f"{prefix}{suffix}"[:20]


def optional_first_name():
    # 50% probabilidad de retornar un nombre o None
    return fake.first_name() if random.random() < 0.8312 else None


def conditional_last_name(first_name):
    # Si first_name existe, generar un apellido; si no, dejarlo nulo
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
    """Remueve caracteres especiales y pone en minúscula"""
    return re.sub(r'[^a-zA-Z0-9]', '', s or "").lower()

def email_from_name_company(company=None, first_name=None, last_name=None):
    """
    Genera un email en el formato user.name@company.com con 74.6% de probabilidad de ser None.
    Usa los campos disponibles: first_name, last_name, company.
    """
    if random.random() < 0.746:
        return None

    # Limpiar y preparar los inputs
    company = clean_string(company)
    first_name = clean_string(first_name)
    last_name = clean_string(last_name)

    # Generar parte del username
    if first_name and last_name:
        user_part = f"{first_name}.{last_name}"
    elif first_name:
        user_part = first_name
    elif last_name:
        user_part = last_name
    else:
        user_part = fake.user_name()

    # Dominio
    domain_part = f"{company}.com" if company else fake.free_email_domain()

    # Email final
    return f"{user_part}@{domain_part}"

