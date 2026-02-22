import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/"
FILE_PATTERN = "NIMF-MONTHLY-PORTFOLIO-{day}-{month}-{year}.xls"


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
        month_short = month_end.strftime("%b")  # Jan
        year_short = month_end.strftime("%y")   # 26

        months.append((day, month_short, year_short))

    return months


def download_file(url, save_path):

    print("Trying:", os.path.basename(save_path))

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

    for day, month, year in months:

        file_name = FILE_PATTERN.format(
            day=day,
            month=month,
            year=year
        )

        url = BASE_URL + file_name
        save_path = os.path.join(save_dir, file_name)

        download_file(url, save_path)

    print("\nNippon Backfill completed.")


if __name__ == "__main__":
    run_backfill()