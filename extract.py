# -*- coding: utf-8 -*-
# =============================================================================
# Extract nested values from a JSON tree.
# https://hackersandslackers.com/extract-data-from-complex-json-python/
# good also first to dig through the json a bit here https://jsonlint.com/
# =============================================================================

def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr
    
        values = extract(obj, arr, key)
        return values