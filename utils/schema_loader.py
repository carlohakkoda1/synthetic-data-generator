import os
import pandas as pd

def load_all_schemas(definitions_dir="definitions"):
    schema_dict = {}

    for file in os.listdir(definitions_dir):
        if file.endswith(".xlsx"):
            domain = file.replace(".xlsx", "")
            df = pd.read_excel(os.path.join(definitions_dir, file))

            df["TYPE"] = df["TYPE"].astype(str).str.lower().replace("text", "varchar").replace("number", "int")
            df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(10).astype(int)

            schema_dict[domain] = df

    return schema_dict
