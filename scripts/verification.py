"""Exact event counts and bounded floating-point checks for synthetic reports."""
import math


def verify_window(key, actual, expected):
    for field in ("impressions", "clicks", "conversions"):
        if actual[field] != expected[field]:
            raise ValueError(f"Window {key} {field}: {actual[field]} != {expected[field]}")
    for field in ("spend", "revenue"):
        value = actual[field]
        if not math.isfinite(value) or not math.isclose(
            value, expected[field], rel_tol=1e-9, abs_tol=1e-9
        ):
            raise ValueError(f"Window {key} {field}: {value} != {expected[field]}")
