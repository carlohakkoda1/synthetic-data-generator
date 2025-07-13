"""
main.py
--------
Entry point for synthetic data generation.

This script reads the table configuration, loads schema definitions,
and generates synthetic data for each table as specified in the config file.
Output is written in chunks as CSV files per domain/table.

See: utils/table_generator.py for table generation logic.

Usage:
    python main.py
"""

import os
import time
import pandas as pd
from utils.schema_loader import load_all_schemas
from utils.logger import log_table_structure
from utils.table_generator import generate_table, load_domain_rules  # <--- Import from utils

OUTPUT_DIR = "output"
SCHEMA_DIR = "definitions"
CONFIG_PATH = "config/table_config.xlsx"
CHUNK_SIZE = 5000


def main():
    """
    Main workflow for generating synthetic data as defined in config/table_config.xlsx.
    """
    schemas = load_all_schemas(SCHEMA_DIR)
    config_df = pd.read_excel(CONFIG_PATH)
    config_df = config_df.sort_values(by="GEN_ORDER")
    print(config_df)

    for _, row in config_df.iterrows():
        domain = row["DOMAIN"]
        table_name = row["TABLE_NAME"]
        num_rows = int(row["ROWS"])
        print(f"🔧 Generating {num_rows} rows for {domain}.{table_name}")
        t0 = time.time()
        
        rules_module = load_domain_rules(domain)
        df_schema = schemas[domain]
        log_table_structure(df_schema[df_schema["TABLE_NAME"] == table_name], rules_module)

        output_path = os.path.join(OUTPUT_DIR, domain)
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, f"{table_name}.csv")
        if os.path.exists(file_path):
            os.remove(file_path)  # Avoid accidental append

        for start in range(0, num_rows, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE, num_rows)
            rows_this_chunk = end - start
            df_data = generate_table(df_schema, table_name, rows_this_chunk, rules_module, domain)

            column_order = row.get("COLUMN_ORDER")
            if pd.notna(column_order):
                ordered_cols = [col.strip() for col in column_order.split(",")]
                df_data = df_data[ordered_cols]

            # Write header only for the first chunk
            df_data.to_csv(file_path, mode='a', index=False, header=(start == 0))
            print(f"  ✅ Chunk {start+1}-{end} saved: {file_path}")

        print(f"⏱️ Table {table_name} generated in {time.time() - t0:.2f} seconds")


if __name__ == "__main__":
    start_total = time.time()
    main()
    print(f"⏱️ Total execution time: {time.time() - start_total:.2f} seconds")
