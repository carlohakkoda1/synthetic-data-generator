import os
import random
import importlib
import pandas as pd
from faker import Faker

from utils.schema_loader import load_all_schemas
from generators.base_rules import get_generator
from utils.logger import log_table_structure
from utils.foreign_key_util import get_foreign_values

# === GLOBALS ===
fake = Faker()
OUTPUT_DIR = "output"

# === LOAD SCHEMAS ONCE ===
schemas = load_all_schemas("definitions")


def load_domain_rules(domain):
    """
    Dynamically loads a Python module for domain-specific rules if available.
    """
    try:
        module = importlib.import_module(f"generators.custom_rules.{domain}_rules")
        return module
    except ModuleNotFoundError:
        print(f"[⚠️ WARNING] No custom rules found for domain: {domain}")
        return None


def generate_table(df_schema, table_name, num_rows, rules_module=None, domain=None):
    """
    Generates synthetic data for a given table using its schema definition and rules.
    Handles foreign keys, faker functions, and custom rules.
    """
    df_table = df_schema[df_schema["TABLE_NAME"] == table_name]
    data = {col["COLUMN_NAME"]: [] for _, col in df_table.iterrows()}

    for _ in range(num_rows):
        current_row = {}

        for _, col in df_table.iterrows():
            col_name = col["COLUMN_NAME"]
            col_type = col["TYPE"]
            length = int(col["LENGTH"])
            rule = col.get("FAKE_RULE", None)

            value = None  # Default value

            # --- Rule type: None ---
            if pd.isna(rule):
                value = None

            # --- Rule type: Foreign Key ---
            elif isinstance(rule, str) and rule.strip().startswith("foreign_key("):
                try:
                    fk_target = rule[len("foreign_key("):-1].strip()
                    target_table, target_col = fk_target.split(".")
                    target_path = os.path.join(OUTPUT_DIR, domain, f"{target_table}.csv")
                    candidates = get_foreign_values(target_path, target_col)
                    value = random.choice(candidates) if candidates else ""
                except Exception as e:
                    print(f"[⚠️ ERROR foreign key] {col_name}: {rule} -> {e}")
                    value = ""

            # --- Rule type: Faker ---
            elif isinstance(rule, str) and rule.strip().startswith("faker."):
                try:
                    func_call = rule.strip()[6:]
                    value = str(eval(f"fake.{func_call}"))[:length]
                except Exception as e:
                    print(f"[⚠️ ERROR faker] {col_name}: {rule} -> {e}")
                    value = ""

            # --- Rule type: Custom Rule in Module ---
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

            # --- Fallback: Default Generator by Type ---
            else:
                value = get_generator(col_type, length)()

            current_row[col_name] = value

        for col_name in current_row:
            data[col_name].append(current_row[col_name])

    return pd.DataFrame(data)


def main():
    """
    Main driver: loads table config, iterates by generation order, applies rules, saves output.
    """
    config_df = pd.read_excel("config/table_config.xlsx")
    config_df = config_df.sort_values(by="GEN_ORDER")

    print(config_df)

    for _, row in config_df.iterrows():
        domain = row["DOMAIN"]
        table_name = row["TABLE_NAME"]
        num_rows = int(row["ROWS"])

        print(f"🔧 Generating {num_rows} rows for {domain}.{table_name}")

        rules_module = load_domain_rules(domain)
        df_schema = schemas[domain]
        log_table_structure(df_schema[df_schema["TABLE_NAME"] == table_name], rules_module)

        df_data = generate_table(df_schema, table_name, num_rows, rules_module, domain)

        output_path = os.path.join(OUTPUT_DIR, domain)
        os.makedirs(output_path, exist_ok=True)

        # Optional column order
        column_order = row.get("COLUMN_ORDER")
        if pd.notna(column_order):
            ordered_cols = [col.strip() for col in column_order.split(",")]
            df_data = df_data[ordered_cols]

        df_data.to_csv(os.path.join(output_path, f"{table_name}.csv"), index=False)
        print(f"✅ File saved successfully: {output_path}/{table_name}.csv")


if __name__ == "__main__":
    main()
