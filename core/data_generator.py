"""
Functions for generating synthetic data tables, including chunked writing and rule application.
"""

import os
import pandas as pd
import time
import inspect
from faker import Faker

from generators.base_rules import get_generator

fake = Faker()
OUTPUT_DIR = "output"


def generate_table(df_schema, table_name, num_rows, rules_module=None, domain=None):
    """
    Generates a DataFrame for a table using the provided schema and rules.

    Args:
        df_schema (pd.DataFrame): DataFrame describing the table schema.
        table_name (str): Name of the table to generate data for.
        num_rows (int): Number of rows to generate.
        rules_module (module, optional): Module with custom rule functions.
        domain (str, optional): Domain for the table (used for rule lookup).

    Returns:
        pd.DataFrame: Generated data for the table.
    """
    df_table = df_schema[df_schema["TABLE_NAME"] == table_name]
    data = {col["COLUMN_NAME"]: [] for _, col in df_table.iterrows()}

    for row in range(num_rows):
        current_row = {}
        for _, col in df_table.iterrows():
            col_name = col["COLUMN_NAME"]
            col_type = col["TYPE"]
            length = int(col["LENGTH"])
            rule = col.get("FAKE_RULE", None)

            value = None

            if pd.isna(rule):
                value = None

            # If the rule starts with "faker.", use the Faker library.
            elif isinstance(rule, str) and rule.strip().startswith("faker."):
                try:
                    func_call = rule.strip()[6:]
                    # eval is safe here because rules come from controlled configs
                    value = str(eval(f"fake.{func_call}"))[:length]
                except Exception as e:
                    print(f"[⚠️ ERROR faker] {col_name}: {rule} -> {e}")
                    value = ""

            # If a custom rule is provided and available in the rules_module
            elif isinstance(rule, str) and rules_module:
                try:
                    func_name = rule.split("(")[0]
                    args_str = rule[len(func_name)+1:-1]
                    args = []
                    if args_str.strip():
                        for arg in args_str.split(","):
                            arg = arg.strip()
                            # If the argument is a string literal, eval it; otherwise, pull from current_row
                            if arg.startswith('"') or arg.startswith("'"):
                                args.append(eval(arg))
                            else:
                                args.append(current_row.get(arg, ""))
                    func = getattr(rules_module, func_name)

                    # Pass row index only if the function expects it
                    sig = inspect.signature(func)
                    if 'row_nums' in sig.parameters:
                        value = func(*args, row_nums=row)
                    else:
                        value = func(*args)

                except Exception as e:
                    print(f"[⚠️ ERROR custom rule] {col_name}: {rule} -> {e}")
                    value = ""

            else:
                value = get_generator(col_type, length)()

            current_row[col_name] = value

        for col_name in current_row:
            data[col_name].append(current_row[col_name])

    return pd.DataFrame(data)


def generate_table_chunked(df_schema, table_name, total_rows, rules_module=None, domain=None, chunk_size=5000, column_order=None):
    """
    Generates table data in chunks and writes to CSV in OUTPUT_DIR.

    Args:
        df_schema (pd.DataFrame): DataFrame with schema.
        table_name (str): Table name.
        total_rows (int): Total number of rows to generate.
        rules_module (module, optional): Custom rules module.
        domain (str, optional): Domain for folder output.
        chunk_size (int, optional): Number of rows per chunk.
        column_order (list, optional): Order of columns for output.

    Returns:
        None
    """
    output_path = os.path.join(OUTPUT_DIR, domain)
    os.makedirs(output_path, exist_ok=True)
    csv_path = os.path.join(output_path, f"{table_name}.csv")

    # Delete previous output file if it exists
    if os.path.exists(csv_path):
        os.remove(csv_path)

    rows_written = 0
    while rows_written < total_rows:
        rows_to_generate = min(chunk_size, total_rows - rows_written)
        print(f"  [Chunk] Generating rows {rows_written+1}-{rows_written+rows_to_generate} for {domain}.{table_name}...")
        t_chunk = time.perf_counter()
        df_chunk = generate_table(
            df_schema, table_name, rows_to_generate, rules_module, domain
        )
        if column_order is not None:
            df_chunk = df_chunk[column_order]
        df_chunk.to_csv(csv_path, mode='a', header=(rows_written == 0), index=False)
        print(f"  [Chunk] Chunk time: {time.perf_counter()-t_chunk:.2f}s")
        rows_written += rows_to_generate
    print(f"[✅] File saved in chunks: {csv_path}")
