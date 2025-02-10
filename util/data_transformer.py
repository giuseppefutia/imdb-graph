import json
import pandas as pd

def split_list(column):
    """Splits comma-separated strings into a list and removes whitespace."""
    return column.apply(lambda x: [item.strip() for item in x.split(",")] if isinstance(x, str) else [])

def split_list_with_nan(column):
    """Splits comma-separated strings into a list, handling NaN values."""
    return column.fillna("").apply(lambda x: [item.strip() for item in x.split(",")] if x else [])

def replace_na(column, default="not_specified"):
    """Replaces NaN or '\\N' values with a specified default."""
    return column.replace(["\\N", None, pd.NA], default)

def to_integer(column):
    """Converts a column to integers, handling non-numeric values safely."""
    return column.apply(lambda x: int(x) if isinstance(x, str) and x.isdigit() else None)

def from_str_list_to_json(column):
    """Converts string list such as '[a,b,c]' into a json structure."""
    return column.apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else x)
