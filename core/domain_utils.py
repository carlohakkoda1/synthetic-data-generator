"""
Helpers for domain-specific operations, such as loading custom rule modules.
"""

import importlib


def load_domain_rules(domain):
    """
    Dynamically imports and returns the custom rules module for a given domain.

    Args:
        domain (str): The name of the domain (e.g., 'customer', 'vendor').

    Returns:
        module or None: The imported rules module, or None if not found.
    """
    try:
        module = importlib.import_module(f"generators.custom_rules.{domain}_rules")
        return module
    except ModuleNotFoundError:
        print(f"[⚠️ WARNING] No custom rules found for domain: {domain}")
        return None