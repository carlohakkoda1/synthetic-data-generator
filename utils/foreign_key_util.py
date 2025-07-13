import pandas as pd
import os
import random
from collections import defaultdict
import time #quitar


# Global cache to avoid reloading files multiple times
_foreign_key_cache = defaultdict(list)

def get_foreign_values(csv_path, column_name):
    import pandas as pd
    try:
        df = pd.read_csv(csv_path, dtype={column_name: str})
        vals = df[column_name].dropna().astype(str).unique().tolist()
        return vals
    except Exception as e:
        print(f"[⚠️ ERROR loading foreign key] {csv_path}.{column_name}: {e}")
        return []




def get_foreign_values_original(csv_path, column_name):
    key = f"{csv_path}.{column_name}"
    if key not in _foreign_key_cache:
        try:
            df = pd.read_csv(csv_path)
            _foreign_key_cache[key] = df[column_name].dropna().unique().tolist()
        except Exception as e:
            print(f"[⚠️ ERROR loading foreign key] {key}: {e}")
    return _foreign_key_cache[key]
