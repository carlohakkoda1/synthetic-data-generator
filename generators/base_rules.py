from faker import Faker
import random

fake = Faker()


def get_generator(dtype: str, length: int = 10):
    dtype = dtype.lower()

    if "varchar" in dtype:
        return lambda: fake.word()[:length]

    elif "int" in dtype:
        return lambda: str(random.randint(10**(length-1), 10**length - 1)) if length > 1 else str(random.randint(0, 9))

    else:
        return lambda: "UNKNOWN"
