import requests
import os
import re
import logging
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.invescomutualfund.com/api/CompleteMonthlyHoldings"
CLASSIFICATION = "equity"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

RETRY_COUNT = 3
DOWNLOAD_TIMEOUT = 60
RATE_LIMIT = 0.5


log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "invesco_scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


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

    for attempt in range(RETRY_COUNT):

        try:

            response = requests.get(
                BASE_URL,
                params=params,
                headers=HEADERS,
                timeout=DOWNLOAD_TIMEOUT
            )

            if response.status_code == 200:
                logger.info(f"API success for {year}")
                return response.json()

            logger.warning(f"API status {response.status_code} for {year}")

        except Exception as e:
            logger.warning(f"API retry {attempt+1} for {year} | {e}")

        time.sleep(2)

    raise Exception(f"API failed after retries for {year}")


def generate_months(n_months=12):

    months = []

    today = datetime.today()

    start_date = today - relativedelta(months=1)

    for i in range(n_months):

        date = start_date - relativedelta(months=i)

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


def download_file(file_url, base_save_dir, fund_name, month, year):

    month_dir = os.path.join(base_save_dir, year, month)
    os.makedirs(month_dir, exist_ok=True)

    std_name = f"{fund_name}_{month}_{year}.xlsx"
    save_path = os.path.join(month_dir, std_name)

    if os.path.exists(save_path):
        logger.info(f"Already exists: {std_name}")
        return

    logger.info(f"Downloading: {std_name}")

    for attempt in range(RETRY_COUNT):

        try:

            r = requests.get(
                file_url,
                timeout=DOWNLOAD_TIMEOUT,
                headers=HEADERS
            )

            if r.status_code == 200:

                if len(r.content) < 1000:
                    logger.warning(f"File seems corrupted: {std_name}")
                    return

                with open(save_path, "wb") as f:
                    f.write(r.content)

                logger.info(f"Saved -> {save_path}")
                return

            logger.warning(f"Status {r.status_code} for {std_name}")

        except Exception as e:
            logger.warning(f"Retry {attempt+1} for {std_name} | {e}")

        time.sleep(2)

    logger.error(f"Download failed after retries: {std_name}")


def run_backfill():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(
        base_dir,
        "data",
        "raw_files",
        "invesco_scraped"
    )

    os.makedirs(save_dir, exist_ok=True)

    rolling_months = generate_months()

    logger.info(f"Rolling months: {rolling_months}")

    years_needed = list(set([y for _, y in rolling_months]))

    logger.info(f"Years needed: {years_needed}")

    fund_data_all = {}

    for year in years_needed:

        logger.info(f"Fetching API → {year}")
        fund_data_all[year] = fetch_fund_data(year)

    logger.info("Starting download for rolling 12 months")

    for year, funds in fund_data_all.items():

        for fund in funds:

            fund_name_raw = fund.get("Name", "")
            fund_name = clean_fund_name(fund_name_raw)

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
                    save_dir,
                    fund_name,
                    mon,
                    yr
                )

                time.sleep(RATE_LIMIT)

    logger.info("Backfill completed.")


if __name__ == "__main__":
    run_backfill()