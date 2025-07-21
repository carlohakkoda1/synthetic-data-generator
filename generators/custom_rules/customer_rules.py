 """
 customer_data_utils.py

 Utility functions for generating synthetic customer master data for SAP S/4HANA
 migration mock‑loads.

 Main themes
 -----------
 * Sequential customer‑number generator
 * Address helpers backed by a curated JSON pool
 * Person, email and phone number generators (US & Canada)
 * Foreign‑key helpers for look‑ups across generated CSV extracts
 * Misc. business‑rule helpers (sales org, payment terms, etc.)

 """

 from __future__ import annotations

 # Standard library
 import json
 import os
 import random
 import re
 from collections import defaultdict
 from pathlib import Path
 from typing import Dict, Generator, List, Optional

 # Third‑party libraries
 import pandas as pd
 import pytz
 from faker import Faker

 # Local imports
 from utils.foreign_key_util import get_foreign_values

 __all__ = [
     # Generators
     "get_customer_number",
     "get_random_timezone",
     # Address helpers
     "get_street",
     "get_post_code1",
     "get_city1",
     "get_country",
     "get_region",
     "get_langu_corr",
     # Person / contact
     "email_from_name_company",
     "mobile_by_country",
     "telephone_by_country",
     # Business logic
     "random_sales_org",
     "get_currency",
     "get_delivery_plant",
     "random_account_assignment_group",
     "get_sales_org",
     "get_company_code",
     "get_payment_terms",
     "get_payment_method",
     "get_house_bak",
     "get_random_reconciliation_account",
     "get_country_region",
     "get_tax_category",
     "get_tax_classification",
     # Foreign‑key helpers
     "foreign_key",
     "lookup_parent_value",
 ]

 # -----------------------------------------------------------------------------
 # Globals & configuration
 # -----------------------------------------------------------------------------
 OUTPUT_DIR = Path("output")
 DOMAIN = "customer"
 RESOURCE_PATH = Path(__file__).resolve().parent / "../resources/address_data.json"

 # Faker contexts
 faker_us = Faker()
 faker_ca = Faker("en_CA")

 # Caches / pools
 _pk_cache: Dict[str, List[str]] = defaultdict(list)
 _pk_generator: Dict[str, List[str]] = defaultdict(list)
 _row_generator_cache: Dict[int, Dict[str, str]] = {}
 _lookup_cache: Dict[str, Dict[str, pd.Series]] = {}

 # -----------------------------------------------------------------------------
 # Time zone helpers
 # -----------------------------------------------------------------------------
 def get_random_timezone(country: str = "USA") -> str:
     """
     Return a random IANA timezone for a given country.

     Parameters
     ----------
     country : {'USA', 'CANADA'}
         Country whose timezone should be returned. Case‑insensitive.
     """
     iso = {"USA": "US", "CANADA": "CA"}.get(country.upper())
     if iso is None:
         raise ValueError("Supported countries are 'USA' and 'CANADA'.")
     return random.choice(pytz.country_timezones[iso])

 # -----------------------------------------------------------------------------
 # Customer‑number generator
 # -----------------------------------------------------------------------------
 def _customer_number_generator(start: int = 100_000_000,
                                end: int = 299_999_999) -> Generator[int, None, None]:
     """Yield unique, sequential customer numbers within *[start, end]*."""
     yield from range(start, end + 1)

 _customer_numbers = _customer_number_generator()

 def get_customer_number() -> int:
     """Return the next sequential customer number."""
     return next(_customer_numbers)

 # -----------------------------------------------------------------------------
 # Address helpers
 # -----------------------------------------------------------------------------
 with open(RESOURCE_PATH, "r") as f:
     _ADDRESS_POOL = json.load(f)
 _STREET_LOOKUP = {addr["STREET"]: addr for addr in _ADDRESS_POOL}

 def get_street() -> str:
     """Return a random street from the address pool."""
     return random.choice(list(_STREET_LOOKUP.keys()))

 def _addr_lookup(street: str, key: str) -> str:
     return _STREET_LOOKUP.get(street, {}).get(key, "")

 def get_post_code1(street: str) -> str:
     return _addr_lookup(street, "POST_CODE1")

 def get_city1(street: str) -> str:
     return _addr_lookup(street, "CITY1")

 def get_country(street: str) -> str:
     return _addr_lookup(street, "COUNTRY")

 def get_region(street: str) -> str:
     return _addr_lookup(street, "REGION")

 def get_langu_corr(street: str) -> str:
     return _addr_lookup(street, "LANGU_CORR")

 # -----------------------------------------------------------------------------
 # Person / contact helpers
 # -----------------------------------------------------------------------------
 _EMAIL_BLOCK_PROB = 0.746  # 25.4 % chance of returning an email
 _PHONE_BLOCK_PROB = 0.2924 # 70.8 % chance of returning a phone

 def _clean_string(token: Optional[str]) -> str:
     """Remove non‑alphanum chars and lowercase."""
     return re.sub(r"[^a-zA-Z0-9]", "", token or "").lower()

 def email_from_name_company(company: Optional[str] = None,
                             first_name: Optional[str] = None,
                             last_name: Optional[str] = None) -> Optional[str]:
     """
     Generate a deterministic email address from *company* and/or *name*.

     Returns *None* with probability ``_EMAIL_BLOCK_PROB`` to mimic missing data.
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

     domain_part = f"{company_clean}.com" if company_clean else faker_us.free_email_domain()
     return f"{user_part}@{domain_part}"

 def _us_ca_mobile() -> str:
     """Return a pseudo‑random US/CA mobile in E.164 lite: +1-XXX-XXX-XXXX."""
     return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"

 def mobile_by_country(country: str) -> Optional[str]:
     """Return a mobile number or *None* based on ``_PHONE_BLOCK_PROB``."""
     if random.random() < _PHONE_BLOCK_PROB:
         return None
     if country.upper() in {"USA", "CANADA"}:
         return _us_ca_mobile()
     return None

 def telephone_by_country(country: str) -> Optional[str]:
     """Return a landline number or *None* based on ``_PHONE_BLOCK_PROB``."""
     if random.random() < _PHONE_BLOCK_PROB:
         return None
     if country.upper() == "USA":
         return faker_us.phone_number()
     if country.upper() == "CANADA":
         return faker_ca.phone_number()
     return None

 # -----------------------------------------------------------------------------
 # Business‑rule helpers
 # -----------------------------------------------------------------------------
 def random_sales_org() -> str:
     return random.choice(["1710", "2930"])

 def get_currency(sales_org: str) -> str:
     return "USD" if sales_org == "1710" else "CAD"

 def get_delivery_plant(sales_org: str) -> str:
     return "US18" if sales_org == "1710" else "CA01"

 def random_account_assignment_group() -> str:
     return random.choice(["1", "2"])

 def get_sales_org(table: str, fk: str, col: str, fk_value: int) -> str:
     """Derive sales org based on the *country* stored in a parent table."""
     return "1710" if lookup_parent_value(table, fk, col, fk_value) == "USA" else "2930"

 def get_company_code(table: str, fk: str, col: str, fk_value: int) -> str:
     """Same logic as *get_sales_org*; exposed separately for clarity."""
     return get_sales_org(table, fk, col, fk_value)

 def get_payment_terms() -> str:           # noqa: D401
     """Return the default payment terms."""
     return "NT30"

 def get_payment_method() -> str:          # noqa: D401
     """Return the default payment method."""
     return "NT30"

 def get_house_bak(company_code: str) -> str:
     """Return main house bank keyed by company code."""
     return "043000096" if company_code == "1710" else random.choice(
         ["26002532", "089906629", "001000001"]
     )

 def get_random_reconciliation_account() -> str:
     return random.choice(["12100000", "12120000"])

 def get_country_region(table: str, fk: str, col: str, fk_value: int) -> str:
     country = lookup_parent_value(table, fk, col, fk_value)
     return "US" if country == "USA" else "CA"

 def get_tax_category(country_code: str) -> str:
     return "UTXJ" if country_code == "US" else "CTXJ"

 def get_tax_classification() -> str:
     return random.choice(["0", "1"])

 # -----------------------------------------------------------------------------
 # Foreign‑key & lookup helpers
 # -----------------------------------------------------------------------------
 def foreign_key(table_name: str, column_name: str, row_num: int | None = None) -> str:
     """
     Return a referentially‑integral value for *table_name.column_name*.

     The helper keeps per‑column pools in ``_pk_cache`` (read once from the
     generated CSV extract) and ensures that no more than two rows reference the
     same FK value before the pool is reshuffled.
     """
     key = f"{table_name}.{column_name}"
     if key not in _pk_cache:
         target_path = OUTPUT_DIR / DOMAIN / f"{table_name}.csv"
         _pk_cache[key] = get_foreign_values(target_path, column_name)

     if not _pk_cache[key]:
         print(f"[⚠️ WARNING] _pk_cache empty for {key}")
         return ""

     max_attempts = len(_pk_cache[key]) * 2
     for _ in range(max_attempts):
         value = str(random.choice(_pk_cache[key]))
         gen_key = f"{key}.{value}"
         if len(_pk_generator[gen_key]) < 2:
             _pk_generator[gen_key].append(value)
             if row_num is not None:
                 _row_generator_cache[row_num] = {column_name: value}
             return value

     # Shuffle pools after saturation
     print(f"[♻️ RESET] Resetting generators for {key}")
     for k in [k for k in _pk_generator if k.startswith(f"{key}.")]:
         del _pk_generator[k]

     value = str(random.choice(_pk_cache[key]))
     _pk_generator[f"{key}.{value}"].append(value)
     if row_num is not None:
         _row_generator_cache[row_num] = {column_name: value}
     return value

 def _make_lookup_key(table: str, fk: str, domain: str = DOMAIN) -> str:
     return f"{domain}.{table}.{fk}"

 def get_lookup_map(table: str, fk: str, domain: str = DOMAIN) -> Dict[str, pd.Series]:
     """
     Cache a ``{fk_value: row_series}`` map for O(1) look‑ups across extracts.
     """
     key = _make_lookup_key(table, fk, domain)
     if key not in _lookup_cache:
         path = OUTPUT_DIR / domain / f"{table}.csv"
         if not path.exists():
             print(f"[⚠️ WARNING] {path} not found")
             _lookup_cache[key] = {}
         else:
             df = pd.read_csv(path)
             _lookup_cache[key] = {row[fk]: row for _, row in df.iterrows()}
     return _lookup_cache[key]

 def lookup_parent_value(table: str, fk: str, column: str,
                         fk_value: int, domain: str = DOMAIN) -> Optional[str]:
     """
     Look up *column* in *table* for the row whose PK equals *fk_value*.
     """
     row = get_lookup_map(table, fk, domain).get(int(fk_value))
     return row.get(column) if row is not None else None

 # -----------------------------------------------------------------------------
 # D‑U‑N‑S helpers (mock data)
 # -----------------------------------------------------------------------------
 _DUNS_TYPE_TO_ID = {
     "DUN & Bradstreet": "BUP001",
     "DNB Global":       "YDNB01",
     "DNB Domestic":     "YDNB02",
     "DNB Parent":       "YDNB03",
     "DNB Headquarters": "YDNB04",
 }

 def get_type_duns_data() -> str:
     return random.choice(list(_DUNS_TYPE_TO_ID.keys()))

 def get_id_duns_data(type_value: str) -> str:
     return _DUNS_TYPE_TO_ID.get(type_value, "")

 # End of module
