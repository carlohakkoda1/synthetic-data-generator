"""
Module for loading table schema definitions from Excel files.

This module reads all `.xlsx` files inside the given `definitions` directory
and builds a dictionary mapping each domain (based on filename) to a DataFrame
containing its schema. The schema is standardized by normalizing types and
lengths.
"""

import os
import pandas as pd


def load_all_schemas(definitions_dir="definitions"):
    """
    Loads all schema definitions from Excel files in the given directory.

    Each `.xlsx` file is assumed to contain column definitions for a specific
    domain.
    The resulting dictionary has:
        - keys: domain names (based on filename)
        - values: DataFrames with standardized TYPE and LENGTH columns

    Args:
        definitions_dir (str): Path to the directory containing Excel schema
        files.

    Returns:
        dict: A dictionary mapping domain names to pandas DataFrames with
        schema info.
    """
    schema_dict = {}

    for file in os.listdir(definitions_dir):
        if file.endswith(".xlsx"):
            domain = file.replace(".xlsx", "")
            df = pd.read_excel(os.path.join(definitions_dir, file))

            df["TYPE"] = (
                df["TYPE"]
                .astype(str)
                .str.lower()
                .replace("text", "varchar")
                .replace("number", "int")
            )

            df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(10).astype(int)

            schema_dict[domain] = df

    return schema_dict
