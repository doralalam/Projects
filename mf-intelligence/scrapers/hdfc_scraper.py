import requests
import os
import logging
import time
import calendar

from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, unquote


BASE_URL = "https://www.hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio"

API_URL = "https://cms.hdfcfund.com/en/hdfc/api/v2/disclosures/monthfortportfolio"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.hdfcfund.com",
    "Referer": BASE_URL
}

RETRY_COUNT = 3
DOWNLOAD_TIMEOUT = 60
RATE_LIMIT = 0.5


log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "scraper_logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "hdfc_scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def generate_reporting_months(n_months=12):

    months = []

    start_date = datetime.today() - relativedelta(months=1)

    for i in range(n_months):

        date = start_date - relativedelta(months=i)

        months.append((date.year, date.month))

    return months


def fetch_month_files(year, month):

    boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

    fields = {
        "year": str(year),
        "month": str(month),
        "type": "monthly"
    }

    body = ""

    for k, v in fields.items():
        body += f"--{boundary}\r\n"
        body += f'Content-Disposition: form-data; name="{k}"\r\n\r\n'
        body += f"{v}\r\n"

    body += f"--{boundary}--\r\n"

    headers = HEADERS.copy()
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"

    try:

        r = requests.post(
            API_URL,
            headers=headers,
            data=body,
            timeout=DOWNLOAD_TIMEOUT
        )

        if r.status_code != 200:
            logger.warning(f"API status {r.status_code}")
            return []

        data = r.json()

    except Exception as e:
        logger.error(f"API error {e}")
        return []

    urls = []

    def extract(obj):

        if isinstance(obj, dict):
            for v in obj.values():
                extract(v)

        elif isinstance(obj, list):
            for v in obj:
                extract(v)

        elif isinstance(obj, str):
            if obj.startswith("http") and "hdfcfund" in obj:
                urls.append(obj)

    extract(data)

    return list(set(urls))


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
                    logger.warning(f"Corrupt file: {os.path.basename(save_path)}")
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
        "hdfc_scraped"
    )

    os.makedirs(save_dir, exist_ok=True)

    months = generate_reporting_months()

    logger.info("Downloading HDFC Monthly Portfolios")

    for year, month in months:

        month_name = calendar.month_abbr[month]

        month_dir = os.path.join(save_dir, str(year), month_name)

        os.makedirs(month_dir, exist_ok=True)

        logger.info(f"Fetching {month_name} {year}")

        urls = fetch_month_files(year, month)

        if not urls:
            logger.warning(f"No files for {month_name} {year}")
            continue

        for url in urls:

            parsed = urlparse(url)

            file_name = unquote(os.path.basename(parsed.path))

            file_name = file_name.split("?")[0]

            file_name = " ".join(file_name.split())

            save_path = os.path.join(month_dir, file_name)

            if os.path.exists(save_path):
                logger.info(f"Already exists: {file_name}")
                continue

            if try_download(url, save_path):
                time.sleep(RATE_LIMIT)

    logger.info("HDFC Backfill completed")


if __name__ == "__main__":
    run_backfill()