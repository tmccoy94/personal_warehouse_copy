"""Editable reference data shipped with the pipeline package.

These are small, human-maintained lookup files (checked into git, editable without a
rebuild since pipelines/src is bind-mounted). Load them with the helpers here rather
than hard-coding paths.
"""
import csv
from importlib.resources import files


def load_product_category_map() -> dict[str, str]:
    """Return the product-name → category lookup from product_to_category.csv.

    Used by the hubspot_live_import_csv asset to collapse Keystone product names
    (e.g. "Power Checking", "Platinum MasterCard") into HubSpot-facing categories
    (e.g. "Checking", "Credit Card"). Edit the CSV to change the mapping — no code
    change needed.
    """
    text = files(__name__).joinpath("product_to_category.csv").read_text(encoding="utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    mapping: dict[str, str] = {}
    for row in reader:
        product = (row.get("PRODUCT") or "").strip()
        category = (row.get("CATEGORY") or "").strip()
        if product:
            mapping[product] = category
    return mapping
