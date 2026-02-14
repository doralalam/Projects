import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.invescomutualfund.com/docs/default-source/factsheet/"

FILE_PREFIX = "invesco-mf-factsheet---"


def generate_last_12_months():
    """
    Generate last 12 months in "month-year" lowercase format
    Example: january-2026
    """

    months = []

    today = datetime.today()

    for i in range(12):
        date = today - relativedelta(months=i)

        month = date.strftime("%B").lower()   # january
        year = date.strftime("%Y")            # 2026

        months.append(f"{month}-{year}")

    return months


def download_file(file_url, save_path):

    print("Downloading:", file_url)

    r = requests.get(file_url)

    if r.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(r.content)

        print("Saved -> ", save_path)

    else:
        print("Not Available:", file_url)


def run_invesco_backfill():

    # Project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(
        base_dir,
        "data",
        "raw_files",
        "invesco"
    )

    os.makedirs(save_dir, exist_ok=True)

    months = generate_last_12_months()

    print(f"\nDownloading last {len(months)} months Invesco factsheets...\n")

    for m in months:

        file_name = f"{FILE_PREFIX}{m}.pdf"
        file_url = BASE_URL + file_name

        save_path = os.path.join(save_dir, file_name)

        download_file(file_url, save_path)

    print("\nInvesco backfill completed")


if __name__ == "__main__":
    run_invesco_backfill()
