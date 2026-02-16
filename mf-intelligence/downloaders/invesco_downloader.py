import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.invescomutualfund.com/api/CompleteMonthlyHoldings"
CLASSIFICATION = "equity"


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


def download_file(file_url, save_dir):

    file_name = file_url.split("/")[-1].split("?")[0]
    save_path = os.path.join(save_dir, file_name)

    if os.path.exists(save_path):
        print("Already exists:", file_name)
        return

    print("Downloading:", file_name)

    try:
        r = requests.get(file_url, timeout=60)

        if r.status_code == 200:

            with open(save_path, "wb") as f:
                f.write(r.content)

            print("Saved ->", save_path)

        else:
            print("Not Available:", file_name)

    except Exception as e:
        print("Error:", file_name, "|", e)


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

            fund_name = fund["Name"].replace(" ", "_")
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

                download_file(file_url, fund_dir)

    print("\nBackfill completed.")


if __name__ == "__main__":
    run_backfill()
