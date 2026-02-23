import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/"

# Month formatting rules
FULL_MONTHS = {
    "Apr": "April",
    "Jun": "June",
    "Jul": "July",
}

NO_DATE_CASES = {
    ("Nov", "25"),  # November 2025 special case
}


def generate_reporting_months(n_months=12):

    months = []
    start_date = datetime.today() - relativedelta(months=1)

    for i in range(n_months):

        date = start_date - relativedelta(months=i)
        month_end = date + relativedelta(day=31)

        day = month_end.strftime("%d")
        month_short = month_end.strftime("%b")
        month_full = month_end.strftime("%B")
        year_short = month_end.strftime("%y")

        months.append((day, month_short, month_full, year_short))

    return months


def build_filename(day, month_short, month_full, year_short):

    # Case 1: No date case
    if (month_short, year_short) in NO_DATE_CASES:
        return f"NIMF-MONTHLY-PORTFOLIO-{month_full}-{year_short}.xls"

    # Case 2: Full month required
    if month_short in FULL_MONTHS:
        month_name = FULL_MONTHS[month_short]
    else:
        month_name = month_short

    return f"NIMF-MONTHLY-PORTFOLIO-{day}-{month_name}-{year_short}.xls"


def download_file(url, save_path):

    try:
        r = requests.get(url, timeout=60)

        if r.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(r.content)

            print("Downloaded:", os.path.basename(save_path))
            return True

    except Exception as e:
        print("Error:", e)

    return False


def run_backfill():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    save_dir = os.path.join(base_dir, "data", "raw_files", "nippon")
    os.makedirs(save_dir, exist_ok=True)

    months = generate_reporting_months()

    print("\nDownloading Nippon Monthly Portfolios...\n")

    for day, mon_short, mon_full, yr in months:

        file_name = build_filename(day, mon_short, mon_full, yr)

        url = BASE_URL + file_name
        save_path = os.path.join(save_dir, file_name)

        print("Trying:", file_name)

        if not download_file(url, save_path):
            print("Not found:", file_name)

    print("\nNippon Backfill completed.")


if __name__ == "__main__":
    run_backfill()