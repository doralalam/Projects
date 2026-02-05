import csv
import os

def normalize_row(row):
    """
    Normalize a single row from any CSV
    """
    return {
        "stock": row.get("stock", "").upper().strip(),
        "broker": row.get("broker", "").strip(),
        "rating": row.get("rating", "").strip(),
        "target_price": parse_float(row.get("target_price")),
        "previous_price": parse_float(row.get("previous_price")),
        "date": row.get("date", "").strip(),
        "source_url": row.get("source_url", "").strip(),
    }


def parse_float(value):
    if not value or value in ("-", "-1", "0", "0.0"):
        return None
    try:
        return float(value)
    except:
        return None


def load_multiple_csvs(folder_path):
    """
    Load and merge all CSV files from a folder
    """
    all_calls = []

    for file in os.listdir(folder_path):
        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file)

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                normalized = normalize_row(row)
                all_calls.append(normalized)

    return all_calls
