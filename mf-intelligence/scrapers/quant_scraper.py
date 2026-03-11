import requests
import os
import re
import logging
import time
import html

from datetime import datetime
from dateutil.relativedelta import relativedelta


DOWNLOAD_BASE = "https://quantmutual.com/Admin/disclouser/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*"
}

RETRY_COUNT = 3
DOWNLOAD_TIMEOUT = 60
RATE_LIMIT = 0.5


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data", "raw_files", "quant_scraped")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


LOG_FILE = os.path.join(LOG_DIR, "quant_scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def parse_fund_names(json_data):

    html_data = html.unescape(json_data["d"])

    links = re.findall(r"Admin/disclouser/.*?\.xlsx", html_data)

    fund_names = set()

    for link in links:

        file_name = link.split("/")[-1]

        parts = file_name.replace(".xlsx", "").split("_")

        fund_name = "_".join(parts[:-2]).lower()

        fund_names.add(fund_name)

    return sorted(fund_names)


def generate_months(n_months=12):

    months = []

    start_date = datetime.today() - relativedelta(months=1)

    std_mon = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
    }

    for i in range(n_months):

        date = start_date - relativedelta(months=i)

        mon_num = date.strftime("%m")
        mon_name = std_mon[mon_num]
        year = date.strftime("%Y")

        months.append((mon_name, year))

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


def run_backfill(fund_names):

    months = generate_months()

    for mon, year in months:

        month_dir = os.path.join(DATA_DIR, year, mon)

        os.makedirs(month_dir, exist_ok=True)

        for fund_name in fund_names:

            file_name = f"{fund_name}_{mon}_{year}.xlsx"

            url = DOWNLOAD_BASE + file_name

            save_path = os.path.join(month_dir, file_name)

            if os.path.exists(save_path):

                logger.info(f"Already exists: {file_name}")
                continue

            logger.info(f"Trying: {file_name}")

            try_download(url, save_path)

            time.sleep(RATE_LIMIT)


if __name__ == "__main__":

    logger.info("Starting Quant scraper")

    json_data = {"d": "\u003cul\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Aggressive_Hybrid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Aggressive Hybrid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_and_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large \u0026 Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Small_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Small Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Infrastructure_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Infrastructure Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Focused_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Focused Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Liquid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Liquid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Flexi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Flexi cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ELSS_Tax_Saver_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ELSS Tax Saver Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ESG_Integration_Strategy_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ESG Integration Strategy Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Quantamental_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Quantamental Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Value_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Value Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large cap fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Gilt_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Gilt Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Overnight_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Overnight Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Dynamic_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Dynamic Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Business_Cycle_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Business Cycle Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_BFSI_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant BFSI Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Healthcare_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Healthcare Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Manufacturing_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Manufacturing Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Teck_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Teck Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Momentum_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Momentum Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Commodities_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Commodities Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Consumption_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Consumption Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_PSU_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant PSU Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Arbitrage_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Arbitrage Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Equity_Savings_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Equity Savings Fund\u003c/a\u003e\u003c/li\u003e\u003c/ul\u003e"
    }

    fund_names = parse_fund_names(json_data)

    logger.info(f"Found {len(fund_names)} funds")

    run_backfill(fund_names)

    logger.info("Quant scraping completed")