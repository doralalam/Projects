import requests
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.invescomutualfund.com/api/CompleteMonthlyHoldings"
CLASSIFICATION = "equity"


def clean_fund_name(name):

    name = name.strip()
    name = name.replace("&", "and")
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^A-Za-z0-9_]", "", name)

    return name


def fetch_fund_data(year):

    params = {
        "year": year,
        "classification": CLASSIFICATION
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API failed for {year}")

    return response.json()


def generate_months(n_months=12):

    months = []

    today = datetime.today()

    for i in range(n_months):

        date = today - relativedelta(months=i)

        mon = date.strftime("%b")
        year = date.strftime("%Y")

        months.append((mon, year))

    return months


month_key_map = {
    "Jan": ("JanUrl", "JanName"),
    "Feb": ("FebUrl", "FebName"),
    "Mar": ("MarUrl", "MarName"),
    "Apr": ("AprUrl", "AprName"),
    "May": ("MayUrl", "MayName"),
    "Jun": ("JunUrl", "JunName"),
    "Jul": ("JulUrl", "JulName"),
    "Aug": ("AugUrl", "AugName"),
    "Sep": ("SepUrl", "SepName"),
    "Oct": ("OctUrl", "OctName"),
    "Nov": ("NovUrl", "NovName"),
    "Dec": ("DecUrl", "DecName"),
}


def download_file(file_url, save_dir, fund_name, month, year):

    std_name = f"{fund_name}_{month}_{year}.xlsx"
    save_path = os.path.join(save_dir, std_name)

    if os.path.exists(save_path):
        print("Already exists:", std_name)
        return

    print("Downloading:", std_name)

    try:

        r = requests.get(file_url, timeout=60)

        if r.status_code == 200:

            with open(save_path, "wb") as f:
                f.write(r.content)

            print("Saved ->", save_path)

        else:
            print("Not Available:", std_name)

    except Exception as e:
        print("Error:", std_name, "|", e)


def run_backfill():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(
        base_dir,
        "data",
        "raw_files",
        "invesco"
    )

    os.makedirs(save_dir, exist_ok=True)

    rolling_months = generate_months()

    years_needed = list(set([y for _, y in rolling_months]))

    print("\nYears needed:", years_needed)

    fund_data_all = {}

    for year in years_needed:

        print(f"Fetching API → {year}")
        fund_data_all[year] = fetch_fund_data(year)

    print("\nDownloading rolling 12 months...\n")

    for year, funds in fund_data_all.items():

        for fund in funds:

            fund_name_raw = fund.get("Name", "")
            fund_name = clean_fund_name(fund_name_raw)

            fund_dir = os.path.join(save_dir, fund_name)
            os.makedirs(fund_dir, exist_ok=True)

            for mon, yr in rolling_months:

                if str(year) != str(yr):
                    continue

                url_key, name_key = month_key_map[mon]

                file_url = fund.get(url_key)
                month_label = fund.get(name_key)

                if not file_url or not month_label:
                    continue

                download_file(
                    file_url,
                    fund_dir,
                    fund_name,
                    mon,
                    yr
                )

    print("\nBackfill completed.")


if __name__ == "__main__":
    run_backfill()
