import os
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data", "raw_files", "motilal")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "motilal_scraper.log")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def create_month_folders(n_months=12):

    today = datetime.today() - relativedelta(months=1)

    folders = []

    for i in range(n_months):

        date = today - relativedelta(months=i)

        year = date.strftime("%Y")
        month = date.strftime("%b")

        year_path = os.path.join(DATA_DIR, year)
        folder_path = os.path.join(year_path, month)

        if os.path.exists(folder_path):
            logger.info(f"Folder already exists: {folder_path}")
        else:
            os.makedirs(folder_path, exist_ok=True)
            logger.info(f"Created folder: {folder_path}")

        folders.append((folder_path, month, year))

    return folders


def validate_files(folders):

    for folder_path, month, year in folders:

        files = os.listdir(folder_path)

        if len(files) == 0:
            logger.error(
                f"File missing for {month} {year}. Please add the file."
            )


if __name__ == "__main__":

    logger.info("Starting Motilal folder structure setup")

    folders = create_month_folders()

    validate_files(folders)

    logger.info("Motilal validation completed")