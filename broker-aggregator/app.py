
from flask import Flask, render_template, request
from utils.data_loader import load_all_sheets
from datetime import datetime

## Scheduler import
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)


## data cache

## Stores all broker calls
BROKER_CALLS = []

## Stores broker summary (name + call count)
BROKERS = {}

## Stores last refresh timestamp
LAST_REFRESH = None



## broker index

def build_broker_index(calls):
    """
    Build broker summary dictionary.

    Output format:
    {
        broker_name: {
            "name": broker_name,
            "count": total_calls
        }
    }
    """

    brokers = {}

    for c in calls:
        name = c["broker"]

        if name not in brokers:
            brokers[name] = {
                "name": name,
                "count": 0
            }

        brokers[name]["count"] += 1

    return brokers


## reload function

def reload_data():
    """
    Reload all broker calls from Google Sheets.

    Updates:
    - BROKER_CALLS
    - BROKERS
    - LAST_REFRESH
    """

    global BROKER_CALLS, BROKERS, LAST_REFRESH

    print("Reloading data from Google Sheets...")

    # Load fresh data
    BROKER_CALLS = load_all_sheets()

    # Rebuild broker index
    BROKERS = build_broker_index(BROKER_CALLS)

    # Update refresh time
    LAST_REFRESH = datetime.now()

    print(f"Reload complete. {len(BROKER_CALLS)} calls loaded.")



## initial load
reload_data()



## scheduler for auto-reload for every hour

def start_scheduler():
    """
    Starts background scheduler
    Reloads data every 1 hour.
    """

    scheduler = BackgroundScheduler()

    # Run reload_data every 60 minutes
    scheduler.add_job(
        func=reload_data,
        trigger="interval",
        hours=1
    )

    scheduler.start()

    print("⏰ Auto-reload scheduler started (1 hour interval)")


# Start scheduler when app launches
start_scheduler()


## sorting function

def sort_calls(calls, sort_key):
    """
    Sort broker calls based on selected column.
    """

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

    # Default sorting → Broker name
    return sorted(
        calls,
        key=lambda x: (x.get("broker") or "").lower()
    )


## routes

@app.route("/", methods=["GET"])
def landing():
    """
    Landing page with search box.
    """
    return render_template("landing.html")


## manual reload
@app.route("/reload")
def admin_reload():
    """
    Manual reload endpoint.
    Useful for testing or urgent refresh.
    """

    reload_data()

    return {
        "status": "success",
        "total_calls": len(BROKER_CALLS),
        "last_refresh": LAST_REFRESH.strftime("%Y-%m-%d %H:%M:%S")
    }


## search section
@app.route("/search", methods=["GET"])
def search():
    """
    Search broker calls by:
    - Stock symbol
    - Broker name
    """

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

## ratings page

@app.route("/ratings", methods=["GET"])
def ratings():
    """
    Display all broker calls.
    Supports sorting.
    """

    sort_key = request.args.get("sort", "broker")

    calls = BROKER_CALLS[:]
    calls = sort_calls(calls, sort_key)

    return render_template(
        "ratings.html",
        query="",
        calls=calls,
        sort=sort_key
    )


## stock detail page

@app.route("/stock/<symbol>", methods=["GET"])
def stock_detail(symbol):
    """
    Show all broker calls for a specific stock.
    """

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


## broker list

@app.route("/brokers", methods=["GET"])
def brokers_list():
    """
    List all brokers with call counts.
    """

    broker_list = list(BROKERS.values())

    broker_list.sort(key=lambda b: b["name"].lower())

    return render_template(
        "brokers.html",
        brokers=broker_list
    )


## broker details

@app.route("/broker/<broker_name>", methods=["GET"])
def broker_detail(broker_name):
    """
    Show all calls from a specific broker.
    """

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
