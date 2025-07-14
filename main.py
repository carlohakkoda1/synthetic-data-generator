import os
import random
import importlib
import pandas as pd
from faker import Faker
import time

from utils.schema_loader import load_all_schemas
from generators.base_rules import get_generator
from utils.logger import log_table_structure
from utils.foreign_key_util import get_foreign_values

fake = Faker()
OUTPUT_DIR = "output"

def load_domain_rules(domain):
    try:
        module = importlib.import_module(f"generators.custom_rules.{domain}_rules")
        return module
    except ModuleNotFoundError:
        print(f"[⚠️ WARNING] No custom rules found for domain: {domain}")
        return None

def generate_table(df_schema, table_name, num_rows, rules_module=None, domain=None):
    df_table = df_schema[df_schema["TABLE_NAME"] == table_name]
    data = {col["COLUMN_NAME"]: [] for _, col in df_table.iterrows()}

    row_times = []
    fk_times = []
    faker_times = []
    custom_times = []
    gen_times = []

    for _ in range(num_rows):
        current_row = {}

        row_start = time.perf_counter()
        for _, col in df_table.iterrows():
            col_name = col["COLUMN_NAME"]
            col_type = col["TYPE"]
            length = int(col["LENGTH"])
            rule = col.get("FAKE_RULE", None)

            value = None

            if pd.isna(rule):
                value = None

            elif isinstance(rule, str) and rule.strip().startswith("foreign_key("):
                t0 = time.perf_counter()
                try:
                    fk_target = rule[len("foreign_key("):-1].strip()
                    target_table, target_col = fk_target.split(".")
                    target_path = os.path.join(OUTPUT_DIR, domain, f"{target_table}.csv")
                    candidates = get_foreign_values(target_path, target_col)
                    value = random.choice(candidates) if candidates else ""
                except Exception as e:
                    print(f"[⚠️ ERROR foreign key] {col_name}: {rule} -> {e}")
                    value = ""
                fk_times.append(time.perf_counter() - t0)

            elif isinstance(rule, str) and rule.strip().startswith("faker."):
                t0 = time.perf_counter()
                try:
                    func_call = rule.strip()[6:]
                    value = str(eval(f"fake.{func_call}"))[:length]
                except Exception as e:
                    print(f"[⚠️ ERROR faker] {col_name}: {rule} -> {e}")
                    value = ""
                faker_times.append(time.perf_counter() - t0)

            elif isinstance(rule, str) and rules_module:
                t0 = time.perf_counter()
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
                custom_times.append(time.perf_counter() - t0)

            else:
                t0 = time.perf_counter()
                value = get_generator(col_type, length)()
                gen_times.append(time.perf_counter() - t0)

            current_row[col_name] = value

        for col_name in current_row:
            data[col_name].append(current_row[col_name])
        row_times.append(time.perf_counter() - row_start)

    print(f"[⏱️ PERF] Tiempo promedio por fila: {sum(row_times)/len(row_times):.4f} s")
    if fk_times:
        print(f"[⏱️ PERF] Tiempo promedio foreign_key: {sum(fk_times)/len(fk_times):.6f} s")
    if faker_times:
        print(f"[⏱️ PERF] Tiempo promedio faker: {sum(faker_times)/len(faker_times):.6f} s")
    if custom_times:
        print(f"[⏱️ PERF] Tiempo promedio custom rule: {sum(custom_times)/len(custom_times):.6f} s")
    if gen_times:
        print(f"[⏱️ PERF] Tiempo promedio fallback gen: {sum(gen_times)/len(gen_times):.6f} s")

    return pd.DataFrame(data)

def generate_table_chunked(df_schema, table_name, total_rows, rules_module=None, domain=None, chunk_size=5000, column_order=None):
    output_path = os.path.join(OUTPUT_DIR, domain)
    os.makedirs(output_path, exist_ok=True)
    csv_path = os.path.join(output_path, f"{table_name}.csv")

    # Borra archivo previo si existe
    if os.path.exists(csv_path):
        os.remove(csv_path)

    rows_written = 0
    while rows_written < total_rows:
        rows_to_generate = min(chunk_size, total_rows - rows_written)
        print(f"  [Chunk] Generando filas {rows_written+1}-{rows_written+rows_to_generate} para {domain}.{table_name}...")
        t_chunk = time.perf_counter()
        df_chunk = generate_table(
            df_schema, table_name, rows_to_generate, rules_module, domain
        )
        if column_order is not None:
            df_chunk = df_chunk[column_order]
        df_chunk.to_csv(csv_path, mode='a', header=(rows_written == 0), index=False)
        print(f"  [Chunk] Tiempo de chunk: {time.perf_counter()-t_chunk:.2f}s")
        rows_written += rows_to_generate
    print(f"[✅] Archivo guardado por chunks: {csv_path}")

def main():
    start = time.perf_counter()
    print("[PERF] Cargando schemas...")
    schemas = load_all_schemas("definitions")
    print(f"[PERF] Carga de schemas: {time.perf_counter()-start:.2f}s")

    config_start = time.perf_counter()
    config_df = pd.read_excel("config/table_config.xlsx")
    config_df = config_df.sort_values(by="GEN_ORDER")
    print(f"[PERF] Carga de config: {time.perf_counter()-config_start:.2f}s")

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

        print(f"\n[⏳] Generando {num_rows} rows para {domain}.{table_name}...")
        rules_module = load_domain_rules(domain)
        df_schema = schemas[domain]
        log_table_structure(df_schema[df_schema["TABLE_NAME"] == table_name], rules_module)

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
            print(f"[✅] Archivo guardado: {output_path}/{table_name}.csv")
        print(f"[PERF] Tiempo de generación: {time.perf_counter()-t0:.2f}s")

    total = time.perf_counter()-start
    print(f"\n[⏲️ PERF] Tiempo total del script: {total:.2f}s")

if __name__ == "__main__":
    main()
