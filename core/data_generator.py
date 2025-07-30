import os
import pandas as pd
import time
import inspect
from faker import Faker

from generators.base_rules import get_generator


fake = Faker()
OUTPUT_DIR = "output"


def generate_table(df_schema, table_name, num_rows, rules_module=None, domain=None):
    df_table = df_schema[df_schema["TABLE_NAME"] == table_name]
    data = {col["COLUMN_NAME"]: [] for _, col in df_table.iterrows()}

    row_times = []
    fk_times = []
    faker_times = []
    custom_times = []
    gen_times = []

    for row in range(num_rows):
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
                    func = getattr(rules_module, func_name)

                    # ✅ Pasar row solo si la función lo acepta
                    sig = inspect.signature(func)
                    if 'row_nums' in sig.parameters:
                        value = func(*args, row_nums=row)
                    else:
                        value = func(*args)

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