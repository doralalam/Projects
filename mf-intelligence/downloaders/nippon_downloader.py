import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/"


# months with FULL NAMES
FULL_MONTHS = {
    "Apr": "April",
    "Jun": "June",
    "Jul": "July",
}


def generate_reporting_months(n_months=12):
    """
    Generate last N reporting months.
    Reporting month = previous calendar month.
    """
    months = []
    start_date = datetime.today() - relativedelta(months=1)

    for i in range(n_months):

        date = start_date - relativedelta(months=i)

        month_end = date + relativedelta(day=31)

        day = month_end.strftime("%d")
        mon_short = month_end.strftime("%b")
        mon_full = month_end.strftime("%B")
        year_short = month_end.strftime("%y")

        months.append((day, mon_short, mon_full, year_short))

    return months


def build_candidate_filenames(day, mon_short, mon_full, yr):

    candidates = []

    # with date and month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{mon_short}-{yr}.xls")

    # with date and full month (if in FULL_MONTHS)
    if mon_short in FULL_MONTHS:
        candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{FULL_MONTHS[mon_short]}-{yr}.xls")
    else:
        # also try full month anyway
        candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{mon_full}-{yr}.xls")

    # without date and short month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{mon_short}-{yr}.xls")

    # without date and full month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{mon_full}-{yr}.xls")

    return candidates


def try_download(url, save_path):
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

    print("\nDownloading Nippon Monthly Portfolios (multi-pattern)...\n")

    for day, short_mon, full_mon, yr in months:

        candidates = build_candidate_filenames(day, short_mon, full_mon, yr)

        downloaded = False

        for file_name in candidates:

            url = BASE_URL + file_name
            save_path = os.path.join(save_dir, file_name)

            print("Trying:", file_name)

            if try_download(url, save_path):
                downloaded = True
                break

        if not downloaded:
            print(f"Not found for {short_mon} {yr}")

    print("\nNippon Backfill completed")


if __name__ == "__main__":
    run_backfill()