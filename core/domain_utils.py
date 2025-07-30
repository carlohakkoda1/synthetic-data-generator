import importlib

def load_domain_rules(domain):
    try:
        module = importlib.import_module(f"generators.custom_rules.{domain}_rules")
        return module
    except ModuleNotFoundError:
        print(f"[⚠️ WARNING] No custom rules found for domain: {domain}")
        return None