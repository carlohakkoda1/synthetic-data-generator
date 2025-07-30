"""
Main script for orchestrating schema loading, configuration reading, and data generation.
Generates synthetic tables as CSV files based on configuration and schema definitions.
"""

import os
import pandas as pd
import time

from core.schema_loader import load_all_schemas
from core.data_generator import generate_table
from core.data_generator import generate_table_chunked
from core.domain_utils import load_domain_rules

OUTPUT_DIR = "output"

def main():
    """
    Orchestrates the end-to-end data generation process:
    - Loads all schemas
    - Reads the table configuration
    - Generates synthetic data per table and domain (chunked if large)
    - Saves outputs as CSVs in domain-based folders
    """
    start = time.perf_counter()
    print("[PERF] Loading schemas...")
    schemas = load_all_schemas("definitions")
    print(f"[PERF] Schemas loaded in {time.perf_counter()-start:.2f}s")

    config_start = time.perf_counter()
    config_df = pd.read_excel("config/table_config.xlsx")
    config_df = config_df.sort_values(by="GEN_ORDER")
    print(f"[PERF] Config loaded in {time.perf_counter()-config_start:.2f}s")

    CHUNK_SIZE = 5000
    CHUNK_THRESHOLD = 5000

    for _, row in config_df.iterrows():
        domain = row["DOMAIN"]
        table_name = row["TABLE_NAME"]
        num_rows = int(str(row["ROWS"]).replace(',', '').replace('\n', '').strip())
        column_order = None
        col_order_val = row.get("COLUMN_ORDER")
        if pd.notna(col_order_val):
            column_order = [col.strip() for col in col_order_val.split(",")]

        print(f"\n[⏳] Generating {num_rows} rows for {domain}.{table_name}...")
        rules_module = load_domain_rules(domain)
        df_schema = schemas[domain]

        t0 = time.perf_counter()
        if num_rows > CHUNK_THRESHOLD:
            generate_table_chunked(
                df_schema, table_name, num_rows, rules_module, domain,
                chunk_size=CHUNK_SIZE, column_order=column_order
            )
        else:
            df_data = generate_table(df_schema, table_name, num_rows, rules_module, domain)
            if column_order is not None:
                df_data = df_data[column_order]
            output_path = os.path.join(OUTPUT_DIR, domain)
            os.makedirs(output_path, exist_ok=True)
            df_data.to_csv(os.path.join(output_path, f"{table_name}.csv"), index=False)
            print(f"[✅] File saved: {output_path}/{table_name}.csv")
        print(f"[PERF] Generation time: {time.perf_counter()-t0:.2f}s")

    total = time.perf_counter() - start
    print(f"\n[⏲️ PERF] Total script runtime: {total:.2f}s")

if __name__ == "__main__":
    main()
