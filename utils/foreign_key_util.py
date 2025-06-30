import pandas as pd
import os
import random
from collections import defaultdict

# Cache global para evitar recargar archivos muchas veces
_foreign_key_cache = defaultdict(list)


def get_foreign_values(csv_path, column_name):
    key = f"{csv_path}.{column_name}"
    if key not in _foreign_key_cache:
        try:
            df = pd.read_csv(csv_path)
            _foreign_key_cache[key] = df[column_name].dropna().unique().tolist()
        except Exception as e:
            print(f"[⚠️ ERROR loading foreign key] {key}: {e}")
    return _foreign_key_cache[key]
