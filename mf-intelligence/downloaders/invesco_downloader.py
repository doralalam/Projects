import os
import time
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


URL = "https://www.invescomutualfund.com/literature-and-form?tab=Complete"

SAVE_DIR = "data/raw_files/invesco_xlsx"


def get_driver():

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # run in background
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    return driver


def extract_xlsx_links():

    driver = get_driver()

    print("Opening browser...")

    driver.get(URL)

    # Wait for JS to load table
    time.sleep(5)

    links = []

    elements = driver.find_elements(By.TAG_NAME, "a")

    for el in elements:

        href = el.get_attribute("href")

        if href and ".xlsx" in href.lower():
            links.append(href)

    driver.quit()

    print(f"Found {len(links)} XLSX links")

    return links


def download_files(links, limit=17):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(base_dir, "data", "raw_files", "invesco")


    os.makedirs(save_dir, exist_ok=True)

    for link in links[:limit]:

        file_name = link.split("/")[-1].split("?")[0]

        save_path = os.path.join(save_dir, file_name)

        print("Downloading:", file_name)

        r = requests.get(link)

        with open(save_path, "wb") as f:
            f.write(r.content)

    print("\nDownload completed.")


def run():

    links = extract_xlsx_links()

    portfolio_links = [
        l for l in links if "monthly" in l.lower()
    ]

    print("Portfolio files:", len(portfolio_links))

    download_files(portfolio_links, limit=17)


if __name__ == "__main__":
    run()
