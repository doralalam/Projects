from flask import Flask, render_template, request
from utils.data_loader import load_all_sheets
from datetime import datetime


app = Flask(__name__)

# ---------------------------------
# GLOBAL DATA CACHE
# ---------------------------------
BROKER_CALLS = []
BROKERS = {}
LAST_REFRESH = None


## Load broker calls from CSV file at startup
def build_broker_index(calls):
    """
    Build a dict: {broker_name: {"name": broker_name, "count": n}}
    count = how many calls that broker has in our data.
    """
    brokers = {}
    for c in calls:
        name = c["broker"]
        if name not in brokers:
            brokers[name] = {"name": name, "count": 0}
        brokers[name]["count"] += 1
    return brokers

## Reload data
def reload_data():
    """
    Reload all Google Sheets data into memory.
    """
    global BROKER_CALLS, BROKERS, LAST_REFRESH

    print("🔄 Reloading data from Google Sheets...")

    BROKER_CALLS = load_all_sheets()
    BROKERS = build_broker_index(BROKER_CALLS)
    LAST_REFRESH = datetime.now()

    print(f"✅ Reload complete. {len(BROKER_CALLS)} calls loaded.")


reload_data()


# Load once when the app starts
def get_calls():
    return load_all_sheets()

def sort_calls(calls, sort_key):

    if sort_key == "upside":
        return sorted(
            calls,
            key=lambda x: x.get("upside") or 0,
            reverse=True
        )

    if sort_key == "target_price":
        return sorted(
            calls,
            key=lambda x: x.get("target_price") or 0,
            reverse=True
        )

    if sort_key == "date":
        return sorted(
            calls,
            key=lambda x: x.get("date") or "",
            reverse=True
        )

    # Default → broker
    return sorted(
        calls,
        key=lambda x: (x.get("broker") or "").lower()
    )


@app.route("/", methods=["GET"])
def landing():
    """
    Main landing page
    Center title + search box
    """
    return render_template("landing.html")


@app.route("/reload")
def admin_reload():
    reload_data()

    return {
        "status": "success",
        "total_calls": len(BROKER_CALLS),
        "last_refresh": LAST_REFRESH.strftime("%Y-%m-%d %H:%M:%S")
    }



@app.route("/search", methods=["GET"])
def search():

    query = request.args.get("q", "").lower().strip()
    sort_key = request.args.get("sort", "broker")

    if query:
        filtered = [
            c for c in BROKER_CALLS
            if query in (c["stock"] or "").lower()
            or query in (c["broker"] or "").lower()
        ]
    else:
        filtered = BROKER_CALLS.copy()

    filtered = sort_calls(filtered, sort_key)

    return render_template(
        "ratings.html",
        query=query,
        calls=filtered,
        sort=sort_key
    )


@app.route("/ratings", methods=["GET"])
def ratings():

    sort_key = request.args.get("sort", "broker")

    calls = BROKER_CALLS[:]
    calls = sort_calls(calls, sort_key)

    return render_template(
        "ratings.html",
        query="",
        calls=calls,
        sort=sort_key
    )



@app.route("/stock/<symbol>", methods=["GET"])
def stock_detail(symbol):

    symbol = symbol.upper().strip()

    filtered = [
        c for c in BROKER_CALLS
        if c["stock"] == symbol
    ]

    return render_template(
        "stock.html",
        symbol=symbol,
        calls=filtered,
    )


@app.route("/brokers", methods=["GET"])
def brokers_list():
    """
    List all brokers present in our data, with call counts.
    URL: /brokers
    """
    # Convert dict -> list for easy use in template
    broker_list = list(BROKERS.values())

    # Sort by name (optional)
    broker_list.sort(key=lambda b: b["name"].lower())

    return render_template("brokers.html", brokers=broker_list)

@app.route("/broker/<broker_name>", methods=["GET"])
def broker_detail(broker_name):

    calls = [
        c for c in BROKER_CALLS
        if c["broker"] == broker_name
    ]

    return render_template(
        "broker.html",
        broker_name=broker_name,
        calls=calls,
    )



if __name__ == "__main__":
    app.run(debug=True)
