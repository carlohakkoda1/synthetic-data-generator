'''
customer_data_utils.py

Utility functions for generating synthetic customer master data for SAP S/4HANA
migration mock-loads.

Main themes
-----------
* Sequential customer-number generator
* Address helpers backed by a curated JSON pool
* Person, email and phone number generators (US & Canada)
* Foreign-key helpers for look-ups across generated CSV extracts
* Misc. business-rule helpers (sales org, payment terms, etc.)
'''

from __future__ import annotations

# Standard library imports
import json
import random
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Generator, List, Optional

# Third-party library imports
import pandas as pd
import pytz
from faker import Faker

# Local imports
from core.foreign_key_util import get_foreign_values


# ----------------------------------------------------------------------------
# Globals & configuration
# ----------------------------------------------------------------------------
OUTPUT_DIR = Path("output")  # Base output directory for generated CSVs
DOMAIN = "customer"         # Domain for namespacing in output
RESOURCE_PATH = Path(__file__).resolve().parent / "../resources/address_data.json"

# Faker contexts
faker_us = Faker()             # Faker for United States
faker_ca = Faker("en_CA")     # Faker for Canada

# Internal caches to optimize lookups and ensure referential integrity
_pk_cache: Dict[str, List[str]] = defaultdict(list)
_pk_generator: Dict[str, List[str]] = defaultdict(list)
_row_generator_cache: Dict[int, Dict[str, str]] = {}
_lookup_cache: Dict[str, Dict[str, pd.Series]] = {}

# ----------------------------------------------------------------------------
# Time zone helpers
# ----------------------------------------------------------------------------
def get_random_timezone(country: str = "USA") -> str:
    """
    Return a random IANA timezone string for the given country.

    Parameters
    ----------
    country : {'USA', 'CANADA'}
        The country code to sample the timezone from (case-insensitive).

    Returns
    -------
    str
        An IANA timezone identifier, e.g., 'America/New_York'.

    Raises
    ------
    ValueError
        If the provided country is not supported.
    """
    iso_map = {"USA": "US", "CANADA": "CA"}
    iso = iso_map.get(country.upper())
    if iso is None:
        raise ValueError("Supported countries are 'USA' and 'CANADA'.")
    # Select a random timezone for the given ISO country code
    return random.choice(pytz.country_timezones[iso])

# ----------------------------------------------------------------------------
# Customer-number generator
# ----------------------------------------------------------------------------
def _customer_number_generator(
    start: int = 100_000_000,
    end: int = 299_999_999
) -> Generator[int, None, None]:
    """
    Yield unique, sequential customer numbers in the inclusive range [start, end].
    """
    yield from range(start, end + 1)

# Initialize the global generator instance
_customer_numbers = _customer_number_generator()

def get_customer_number() -> int:
    """
    Return the next sequential customer number from the global generator.

    Returns
    -------
    int
        A unique customer number.
    """
    return next(_customer_numbers)

# ----------------------------------------------------------------------------
# Address helpers
# ----------------------------------------------------------------------------
# Load address data pool once at module import
with open(RESOURCE_PATH, "r") as f:
    _ADDRESS_POOL = json.load(f)
_STREET_LOOKUP = {entry["STREET"]: entry for entry in _ADDRESS_POOL}


def get_street() -> str:
    """
    Return a random street name from the loaded address pool.
    """
    return random.choice(list(_STREET_LOOKUP.keys()))


def _addr_lookup(street: str, key: str) -> str:
    """
    Internal helper to fetch address attributes by street name.
    """
    return _STREET_LOOKUP.get(street, {}).get(key, "")


def get_post_code1(street: str) -> str:
    """Return the primary postal code for the given street."""
    return _addr_lookup(street, "POST_CODE1")


def get_city1(street: str) -> str:
    """Return the city name for the given street."""
    return _addr_lookup(street, "CITY1")


def get_country(street: str) -> str:
    """Return the country for the given street."""
    return _addr_lookup(street, "COUNTRY")


def get_region(street: str) -> str:
    """Return the region or state for the given street."""
    return _addr_lookup(street, "REGION")


def get_langu_corr(street: str) -> str:
    """Return the language correction code for the given street."""
    return _addr_lookup(street, "LANGU_CORR")

# ----------------------------------------------------------------------------
# Person / contact helpers
# ----------------------------------------------------------------------------
_EMAIL_BLOCK_PROB = 0.746   # Probability to simulate missing email
_PHONE_BLOCK_PROB = 0.2924  # Probability to simulate missing phone number


def _clean_string(token: Optional[str]) -> str:
    """
    Normalize a string by removing non-alphanumeric characters and lowercasing.
    """
    return re.sub(r"[^a-zA-Z0-9]", "", token or "").lower()


def email_from_name_company(
    company: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None
) -> Optional[str]:
    """
    Deterministically generate an email address from company and/or name.

    Returns None with probability _EMAIL_BLOCK_PROB to simulate missing data.
    """
    if random.random() < _EMAIL_BLOCK_PROB:
        return None

    company_clean = _clean_string(company)
    first = _clean_string(first_name)
    last = _clean_string(last_name)

    if first and last:
        user_part = f"{first}.{last}"
    else:
        user_part = first or last or faker_us.user_name()

    domain = f"{company_clean}.com" if company_clean else faker_us.free_email_domain()
    return f"{user_part}@{domain}"


def _us_ca_mobile() -> str:
    """Generate a pseudo-random US/CA mobile in E.164 lite format."""
    return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"


def mobile_by_country(country: str) -> Optional[str]:
    """
    Return a mobile phone number (US/CA) or None based on _PHONE_BLOCK_PROB.
    """
    if random.random() < _PHONE_BLOCK_PROB:
        return None
    if country.upper() in {"USA", "CANADA"}:
        return _us_ca_mobile()
    return None


def telephone_by_country(country: str) -> Optional[str]:
    """
    Return a landline phone number or None based on _PHONE_BLOCK_PROB.
    """
    if random.random() < _PHONE_BLOCK_PROB:
        return None
    if country.upper() == "USA":
        return faker_us.phone_number()
    if country.upper() == "CANADA":
        return faker_ca.phone_number()
    return None

# ----------------------------------------------------------------------------
# Business-rule helpers
# ----------------------------------------------------------------------------
def random_sales_org() -> str:
    """Pick a random sales organization code."""
    return random.choice(["1710", "2930"])


def get_currency(sales_org: str) -> str:
    """Return currency code based on sales organization."""
    return "USD" if sales_org == "1710" else "CAD"


def get_delivery_plant(sales_org: str) -> str:
    """Return default delivery plant based on sales organization."""
    return "US18" if sales_org == "1710" else "CA01"


def random_account_assignment_group() -> str:
    """Pick a random account assignment group code."""
    return random.choice(["1", "2"])


def get_sales_org(
    table: str,
    fk: str,
    col: str,
    fk_value: int
) -> str:
    """
    Derive sales organization based on country from a parent table lookup.
    """
    country = lookup_parent_value(table, fk, col, fk_value)
    return "1710" if country == "USA" else "2930"


def get_company_code(
    table: str,
    fk: str,
    col: str,
    fk_value: int
) -> str:
    """
    Alias to get_sales_org for deriving company code.
    """
    return get_sales_org(table, fk, col, fk_value)


def get_payment_terms() -> str:
    """Return the default payment terms for customers."""
    return "NT30"


def get_payment_method() -> str:
    """Return the default payment method for customers."""
    return "NT30"


def get_house_bak(company_code: str) -> str:
    """Return main house bank identifier based on company code."""
    if company_code == "1710":
        return "043000096"
    return random.choice(["26002532", "089906629", "001000001"])


def get_random_reconciliation_account() -> str:
    """Pick a random reconciliation account code."""
    return random.choice(["12100000", "12120000"])


def get_country_region(
    table: str,
    fk: str,
    col: str,
    fk_value: int
) -> str:
    """
    Map country name to region code via parent table lookup.
    """
    country = lookup_parent_value(table, fk, col, fk_value)
    return "US" if country == "USA" else "CA"


def get_tax_category(country_code: str) -> str:
    """Return tax category code based on two-letter country code."""
    return "UTXJ" if country_code == "US" else "CTXJ"


def get_tax_classification() -> str:
    """Pick a random tax classification code."""
    return random.choice(["0", "1"])

# ----------------------------------------------------------------------------
# Foreign-key & lookup helpers
# ----------------------------------------------------------------------------
def foreign_key(
    table_name: str,
    column_name: str,
    row_num: int | None = None
) -> str:
    """
    Return a referentially integral foreign key value from a parent CSV extract.

    Ensures no single key is reused more than twice before reshuffling the pool.
    """
    key = f"{table_name}.{column_name}"
    if key not in _pk_cache:
        path = OUTPUT_DIR / DOMAIN / f"{table_name}.csv"
        _pk_cache[key] = get_foreign_values(path, column_name)

    if not _pk_cache[key]:
        print(f"[⚠️ WARNING] _pk_cache empty for {key}")
        return ""

    attempts = len(_pk_cache[key]) * 2
    for _ in range(attempts):
        val = str(random.choice(_pk_cache[key]))
        gen_key = f"{key}.{val}"
        if len(_pk_generator[gen_key]) < 2:
            _pk_generator[gen_key].append(val)
            if row_num is not None:
                _row_generator_cache[row_num] = {column_name: val}
            return val

    # Reset generator if all keys are saturated
    print(f"[♻️ RESET] Resetting generators for {key}")
    for g in list(_pk_generator):
        if g.startswith(f"{key}."):
            del _pk_generator[g]
    # Pick a key after reset
    val = str(random.choice(_pk_cache[key]))
    _pk_generator[f"{key}.{val}"].append(val)
    if row_num is not None:
        _row_generator_cache[row_num] = {column_name: val}
    return val


def _make_lookup_key(table: str, fk: str, domain: str = DOMAIN) -> str:
    """Construct a unique cache key for lookup maps."""
    return f"{domain}.{table}.{fk}"


def get_lookup_map(
    table: str,
    fk: str,
    domain: str = DOMAIN
) -> Dict[str, pd.Series]:
    """
    Cache and return a map from primary key to row series for quick lookups.
    """
    cache_key = _make_lookup_key(table, fk, domain)
    if cache_key not in _lookup_cache:
        path = OUTPUT_DIR / domain / f"{table}.csv"
        if path.exists():
            df = pd.read_csv(path)
            _lookup_cache[cache_key] = {row[fk]: row for _, row in df.iterrows()}
        else:
            print(f"[⚠️ WARNING] {path} not found")
            _lookup_cache[cache_key] = {}
    return _lookup_cache[cache_key]


def lookup_parent_value(
    table: str,
    fk: str,
    column: str,
    fk_value: int,
    domain: str = DOMAIN
) -> Optional[str]:
    """
    Retrieve a specific column value from a parent CSV based on its primary key.
    """
    row = get_lookup_map(table, fk, domain).get(int(fk_value))
    return row.get(column) if row is not None else None

# ----------------------------------------------------------------------------
# D-U-N-S mock data helpers
# ----------------------------------------------------------------------------
_DUNS_TYPE_TO_ID = {
    "DUN & Bradstreet": "BUP001",
    "DNB Global":       "YDNB01",
    "DNB Domestic":     "YDNB02",
    "DNB Parent":       "YDNB03",
    "DNB Headquarters": "YDNB04",
}

def get_type_duns_data() -> str:
    """Pick a random D-U-N-S data type."""
    return random.choice(list(_DUNS_TYPE_TO_ID.keys()))


def get_id_duns_data(type_value: str) -> str:
    """Return the D-U-N-S identifier for a given type."""
    return _DUNS_TYPE_TO_ID.get(type_value, "")

# ----------------------------------------------------------------------------
# Passthrough / default helpers for mapping rules
# ----------------------------------------------------------------------------
def copy_value_from_column(source_value):
    """Passthrough: return the input value unchanged."""
    return source_value


def default(value):
    """Default transformation: return the input value unchanged."""
    return value
