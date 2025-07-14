import pandas as pd
import os
from collections import defaultdict

# === GLOBAL CACHE ===
_foreign_key_cache = defaultdict(list)

def get_foreign_values(csv_path, column_name):
    """
    Loads and caches the unique, non-null values of `column_name` from `csv_path`.

    Args:
        csv_path (str): Path to the CSV file.
        column_name (str): Name of the column to extract values from.

    Returns:
        list: List of unique, non-null values from the column.
    """
    key = f"{csv_path}.{column_name}"
    if key not in _foreign_key_cache:
        try:
            if not os.path.isfile(csv_path):
                print(f"[⚠️ WARNING] File does not exist: {csv_path}")
                return []
            df = pd.read_csv(csv_path)
            if column_name not in df.columns:
                print(f"[⚠️ WARNING] Column '{column_name}' not in {csv_path}")
                return []
            _foreign_key_cache[key] = df[column_name].dropna().unique().tolist()
        except Exception as e:
            print(f"[⚠️ ERROR loading foreign key] {key}: {e}")
            return []
    return _foreign_key_cache[key]
