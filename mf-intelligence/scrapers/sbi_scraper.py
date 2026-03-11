import requests
import os
import logging
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.sbimf.com/docs/default-source/scheme-portfolios/"

URL_PATTERNS = [
    "all-schemes-monthly-portfolio---as-on-{day}-{month}-{year}.xlsx",
    "all-scheme-monthly-portfolio---as-on-{day}-{month}-{year}.xlsx"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*"
}

RETRY_COUNT = 3
DOWNLOAD_TIMEOUT = 60
RATE_LIMIT = 0.5


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data", "raw_files", "sbi_scraped")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


LOG_FILE = os.path.join(LOG_DIR, "sbi_scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_ordinal(day):

    if 11 <= day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

    return f"{day}{suffix}"


def generate_months(n_months=12):

    months = []

    today = datetime.today() - relativedelta(months=1)

    for i in range(n_months):

        date = today - relativedelta(months=i)

        month_end = date + relativedelta(day=31)

        day = month_end.day
        month_full = month_end.strftime("%B")
        month_short = month_end.strftime("%b")
        year = month_end.strftime("%Y")

        months.append((day, month_full, month_short, year))

    return months


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
                    logger.warning(f"File corrupted: {os.path.basename(save_path)}")
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

    rolling_months = generate_months()

    logger.info("Downloading SBI Monthly Portfolios")

    for day, month_full, month_short, year in rolling_months:

        year_dir = os.path.join(DATA_DIR, year)
        month_dir = os.path.join(year_dir, month_short)

        os.makedirs(month_dir, exist_ok=True)

        day_with_suffix = get_ordinal(day)

        downloaded = False

        for pattern in URL_PATTERNS:

            file_name = pattern.format(
                day=day_with_suffix,
                month=month_full,
                year=year
            )

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
            logger.warning(f"Not found for {month_full} {year}")

    logger.info("SBI Backfill completed")


if __name__ == "__main__":

    logger.info("Starting SBI scraper")

    run_backfill()

    logger.info("SBI scraping completed")