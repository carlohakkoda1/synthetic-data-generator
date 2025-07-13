# utils/table_generator.py

import os
import importlib
import random
import pandas as pd
from generators.base_rules import get_generator
from utils.foreign_key_util import get_foreign_values
from faker import Faker

fake = Faker()

OUTPUT_DIR = "output"


def load_domain_rules(domain):
    """
    Dynamically imports the custom rules module for a given domain.
    """
    try:
        module = importlib.import_module(f"generators.custom_rules.{domain}_rules")
        return module
    except ModuleNotFoundError:
        print(f"[⚠️ WARNING] No custom rules found for domain: {domain}")
        return None


def generate_table(df_schema, table_name, num_rows, rules_module=None, domain=None):
    """
    Generates a DataFrame of synthetic data for a specified table,
    based on schema and rules (faker, custom, foreign key, etc).
    """
    df_table = df_schema[df_schema["TABLE_NAME"] == table_name]
    data = {}

    # 1. Vectorized generation for faker.* columns
    for _, col in df_table.iterrows():
        col_name = col["COLUMN_NAME"]
        length = int(col["LENGTH"])
        rule = col.get("FAKE_RULE", None)
        if isinstance(rule, str) and rule.strip().startswith("faker."):
            func_call = rule.strip()[6:]
            try:
                data[col_name] = [str(eval(f"fake.{func_call}"))[:length] for _ in range(num_rows)]
            except Exception as e:
                print(f"[⚠️ ERROR faker] {col_name}: {rule} -> {e}")
                data[col_name] = ["" for _ in range(num_rows)]
        else:
            data[col_name] = [None for _ in range(num_rows)]

    # 2. Row-wise loop for non-faker columns
    for i in range(num_rows):
        current_row = {}
        for _, col in df_table.iterrows():
            col_name = col["COLUMN_NAME"]
            col_type = col["TYPE"]
            length = int(col["LENGTH"])
            rule = col.get("FAKE_RULE", None)

            if pd.isna(rule):
                value = None

            elif isinstance(rule, str) and rule.strip().startswith("faker."):
                value = data[col_name][i]  # Already generated

            elif isinstance(rule, str) and rule.strip().startswith("foreign_key("):
                try:
                    fk_params = rule[len("foreign_key("):-1].strip()
                    params = [p.strip() for p in fk_params.split(",")]
                    fk_target = params[0]
                    relationship_type = params[1].upper() if len(params) > 1 else "1:N"
                    target_table, target_col = fk_target.split(".")
                    target_path = os.path.join(OUTPUT_DIR, domain, f"{target_table}.csv")
                    candidates = get_foreign_values(target_path, target_col)
                    if not candidates:
                        raise ValueError("No foreign key candidates found")
                    if relationship_type == "1:1":
                        if len(candidates) < num_rows:
                            raise ValueError("Not enough FK candidates for 1:1 relationship")
                        value = candidates[i]
                    else:
                        value = random.choice(candidates)
                except Exception as e:
                    print(f"[⚠️ ERROR foreign key] {col_name}: {rule} -> {e}")
                    value = ""

            elif isinstance(rule, str) and rules_module:
                try:
                    func_name = rule.split("(")[0]
                    args_str = rule[len(func_name)+1:-1]
                    args = []
                    if args_str.strip():
                        for arg in args_str.split(","):
                            arg = arg.strip()
                            if arg.startswith('"') or arg.startswith("'"):
                                args.append(eval(arg))
                            else:
                                args.append(current_row.get(arg, ""))
                    value = getattr(rules_module, func_name)(*args)
                except Exception as e:
                    print(f"[⚠️ ERROR custom rule] {col_name}: {rule} -> {e}")
                    value = ""
            else:
                value = get_generator(col_type, length)()

            current_row[col_name] = value
            data[col_name][i] = value  # Update list at position i

    return pd.DataFrame(data)
