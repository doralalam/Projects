import csv
import io
import requests
from utils.sheet_registry import SHEET_URLS


def parse_float(value):
    if not value or value in ("", "-", "-1", "0", "0.0"):
        return None
    try:
        return float(value)
    except:
        return None


def load_single_sheet(url, broker_name):
    """
    Load one Google Sheet CSV
    """

    response = requests.get(url)
    response.raise_for_status()

    csv_text = response.text
    csv_file = io.StringIO(csv_text)

    reader = csv.DictReader(csv_file)

    calls = []

    for row in reader:

        calls.append({
            "stock": (row.get("stock") or "").upper().strip(),
            "broker": broker_name.strip(),
            "rating": (row.get("rating") or "").strip(),
            "target_price": parse_float(row.get("target_price")),
            "previous_price": parse_float(row.get("previous_price")),
            "date": (row.get("date") or "").strip(),
            "source_url": (row.get("source_url") or "").strip(),
        })

    return calls

def load_all_sheets():
    """
    Merge all broker sheets into one dataset
    """

    all_calls = []

    for sheet in SHEET_URLS:

        try:
            calls = load_single_sheet(
                sheet["url"],
                sheet["broker"]
            )

            all_calls.extend(calls)

            print(f"Loaded {len(calls)} calls from {sheet['broker']}")

        except Exception as e:
            print(f"Error loading {sheet['broker']}: {e}")

    print("Total merged calls:", len(all_calls))

    return all_calls
