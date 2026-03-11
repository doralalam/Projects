import requests
import os
import logging
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*"
}

RETRY_COUNT = 3
DOWNLOAD_TIMEOUT = 60
RATE_LIMIT = 0.5

log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "nippon_scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


## Handling the issue with month names not following standard format
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
        year_full = month_end.strftime("%Y")

        months.append((day, mon_short, mon_full, year_short, year_full))

    return months


def build_candidate_filenames(day, mon_short, mon_full, yr):

    candidates = []

    # with date and short month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{mon_short}-{yr}.xls")

    # with date and full month
    if mon_short in FULL_MONTHS:
        candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{FULL_MONTHS[mon_short]}-{yr}.xls")
    else:
        candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{day}-{mon_full}-{yr}.xls")

    # without date and short month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{mon_short}-{yr}.xls")

    # without date and full month
    candidates.append(f"NIMF-MONTHLY-PORTFOLIO-{mon_full}-{yr}.xls")

    return candidates


def try_download(url, save_path):

    for attempt in range(RETRY_COUNT):

        try:

            r = requests.get(
                url,
                headers=HEADERS,
                timeout=DOWNLOAD_TIMEOUT
            )

            if r.status_code == 200:

                if len(r.content) < 1000:
                    logger.warning(f"File seems corrupted: {os.path.basename(save_path)}")
                    return False

                with open(save_path, "wb") as f:
                    f.write(r.content)

                logger.info(f"Downloaded: {os.path.basename(save_path)}")
                return True

            else:
                logger.warning(f"Status {r.status_code} for {url}")

        except Exception as e:
            logger.warning(f"Retry {attempt+1} for {url} | {e}")

        time.sleep(2)

    logger.error(f"Failed after retries: {url}")
    return False


def run_backfill():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(
        base_dir,
        "data",
        "raw_files",
        "nippon_scraped"
    )

    os.makedirs(save_dir, exist_ok=True)

    months = generate_reporting_months()

    logger.info("Downloading Nippon Monthly Portfolios")

    for day, short_mon, full_mon, yr_short, yr_full in months:

        month_dir = os.path.join(save_dir, yr_full, short_mon)
        os.makedirs(month_dir, exist_ok=True)

        candidates = build_candidate_filenames(day, short_mon, full_mon, yr_short)

        downloaded = False

        for file_name in candidates:

            url = BASE_URL + file_name
            save_path = os.path.join(month_dir, file_name)

            if os.path.exists(save_path):
                logger.info(f"Already exists: {file_name}")
                downloaded = True
                break

            logger.info(f"Trying: {file_name}")

            if try_download(url, save_path):
                downloaded = True
                break

            time.sleep(RATE_LIMIT)

        if not downloaded:
            logger.warning(f"Not found for {short_mon} {yr_full}")

    logger.info("Nippon Backfill completed")


if __name__ == "__main__":
    run_backfill()