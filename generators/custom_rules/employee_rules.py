import random


def generate_person_number():
    """
    Generate a synthetic PERSON_NUMBER value mimicking real-world patterns
    observed in your data sample.

    Patterns supported (with approximate probabilities based on your analysis):
      1. Letters followed by numbers (≈63%):
         - Examples: 'CA065870', 'USG54208', 'USX18000'
         - Common prefixes: 'USG', 'USX', 'USWL', 'CA', 'US', 'USWU'
         - The total length is typically 8 characters, rarely 9.
      2. Only numbers (≈36%):
         - Example: '50554793'
         - Usually starts with '50'
         - Total length is 8 digits

    Returns:
        str: A synthetic PERSON_NUMBER value similar to your source data.
    """
    # Choose the pattern based on observed frequency
    pattern = random.choices(
        ["letters_numbers", "only_numbers"],
        weights=[0.63, 0.36],  # Adjusted to match your data distribution
        k=1
    )[0]

    if pattern == "letters_numbers":
        # Common letter prefixes observed in your data
        letter_prefixes = [
            "USG", "USX", "USWL", "CA", "US", "USWU"
        ]
        prefix = random.choice(letter_prefixes)
        # Make total length 8 characters (majority of cases)
        num_length = 8 - len(prefix)
        # If prefix is longer, occasionally allow length 9 (rare in your data)
        if num_length < 3:
            num_length = 9 - len(prefix)
        digits = ''.join(random.choices("0123456789", k=num_length))
        return prefix + digits

    else:  # only_numbers
        # Most numeric IDs start with '50'
        prefix = "50"
        # Fill up to 8 digits
        digits = ''.join(random.choices("0123456789", k=6))
        return prefix + digits

def default(value):
    """Returns the value as is."""
    return value

