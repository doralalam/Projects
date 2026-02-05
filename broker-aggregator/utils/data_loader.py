import csv

def load_broker_calls_from_csv(path: str):


    calls = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["stock"] = row["stock"].upper().strip()

            tp_raw = row["target_price"].strip()

            if tp_raw in ("", "-1", "0", "0.0"):
                row["target_price"] = None
            else:
                try:
                    row["target_price"] = float(tp_raw)
                except:
                    row["target_price"] = None


            pp_raw = row.get("previous_price", "").strip()

            if pp_raw in ("", "-1"):
                row["previous_price"] = None
            else:
                try:
                    row["previous_price"] = float(pp_raw)
                except:
                    row["previous_price"] = None

            calls.append(row)
    return calls
