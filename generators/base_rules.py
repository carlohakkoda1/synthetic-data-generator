"""
Base generator rules for synthetic data creation, mapping types to value generators.
"""

from faker import Faker
import random

fake = Faker()


def get_generator(dtype: str, length: int = 10):
    """
    Returns a generator function for the specified data type and length.

    Args:
        dtype (str): Data type (e.g., 'varchar', 'int').
        length (int, optional): Maximum length/size of the value.

    Returns:
        function: A zero-argument function that returns a synthetic value of the desired type/length.
    """
    dtype = dtype.lower()

    if "varchar" in dtype:
        # Generate a random word truncated to the specified length
        return lambda: fake.word()[:length]

    elif "int" in dtype:
        # Generate a random integer as a string with the specified length
        return lambda: str(random.randint(10**(length-1), 10**length - 1)) if length > 1 else str(random.randint(0, 9))

    else:
        # Default for unsupported types
        return lambda: "UNKNOWN"
