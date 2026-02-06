import csv                      # for reading CSV files
from flask import Flask, render_template, request
from utils.data_loader import load_all_sheets





app = Flask(__name__)


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




# Load once when the app starts
BROKER_CALLS = load_all_sheets()



# Build broker index once
BROKERS = build_broker_index(BROKER_CALLS)

@app.route("/", methods=["GET"])
def landing():
    """
    Main landing page
    Center title + search box
    """
    return render_template("landing.html")


@app.route("/search", methods=["GET"])
def search():
    """
    Home page with search box.
    - If ?q=SYMBOL is provided, filter calls for that symbol.
    - Otherwise, show ALL broker calls.
    Also sort by broker name (ascending).
    """


    query = request.args.get("q", "").lower().strip()

    # Start from all calls
    if query:
        filtered = [
            c for c in BROKER_CALLS
            if query in (c["stock"] or "").lower()
            or query in (c["broker"] or "").lower()
        ]
    else:
        filtered = BROKER_CALLS[:]   # copy full list


    # Sort by broker name ascending
    filtered.sort(key=lambda c: c["broker"].lower())

    return render_template(
        "ratings.html",
        query=query,
        calls=filtered,
    )

@app.route("/ratings", methods=["GET"])
def ratings():
    """
    Broker ratings table page
    """

    calls = BROKER_CALLS[:]
    calls.sort(key=lambda c: (c["broker"] or "").lower())

    return render_template(
        "ratings.html",
        query="",
        calls=calls,
    )


@app.route("/stock/<symbol>", methods=["GET"])
def stock_detail(symbol):
    """
    Stock detail page.
    Example URL: /stock/HDFCBANK
    Shows all broker calls for that stock.
    """
    symbol = symbol.upper().strip()

    # Filter all calls for this symbol
    filtered = [c for c in BROKER_CALLS if c["stock"] == symbol]

    return render_template(
        "stock.html",      # new template we'll create
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
    """
    Show all calls by a given broker.
    We’ll match on the exact broker name from the URL (URL-decoded).
    Example: /broker/HDFC%20Securities
    """
    # Flask gives broker_name URL-decoded, so spaces are spaces again.
    name = broker_name

    # Filter all calls from this broker
    calls = [c for c in BROKER_CALLS if c["broker"] == name]

    return render_template(
        "broker.html",
        broker_name=name,
        calls=calls,
    )



if __name__ == "__main__":
    app.run(debug=True)
