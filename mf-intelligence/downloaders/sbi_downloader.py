import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://www.sbimf.com/docs/default-source/scheme-portfolios/"

# try both the patterns
URL_PATTERNS = [
    "all-schemes-monthly-portfolio---as-on-{day}-{month}-{year}.xlsx",
    "all-scheme-monthly-portfolio---as-on-{day}-{month}-{year}.xlsx"
]


def get_ordinal(day: int) -> str:
    if 11 <= day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def generate_months(n_months=12):
    months = []
    today = datetime.today()-relativedelta(months=1)

    for i in range(n_months):
        date = today - relativedelta(months=i)
        month_end = date + relativedelta(day=31)

        day = month_end.day
        month_full = month_end.strftime("%B")
        year = month_end.strftime("%Y")

        months.append((day, month_full, year))

    return months


def try_download(url, save_path):

    try:
        r = requests.get(url, timeout=60)

        if r.status_code == 200:

            # Always overwrite
            with open(save_path, "wb") as f:
                f.write(r.content)

            print("Downloaded:", os.path.basename(save_path))
            return True

    except Exception as e:
        print("Error:", e)

    return False


def run_backfill():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    save_dir = os.path.join(base_dir, "data", "raw_files", "sbi")
    os.makedirs(save_dir, exist_ok=True)

    rolling_months = generate_months()

    print("\nDownloading SBI rolling 12 months (overwrite mode)...\n")

    for day, month, year in rolling_months:

        day_with_suffix = get_ordinal(day)
        downloaded = False

        for pattern in URL_PATTERNS:

            file_name = pattern.format(
                day=day_with_suffix,
                month=month,
                year=year
            )

            save_path = os.path.join(save_dir, file_name)
            url = BASE_URL + file_name

            if try_download(url, save_path):
                downloaded = True
                break  

        if not downloaded:
            print(f"Not found for {month.title()} {year}")

    print("\nSBI Backfill completed.")


if __name__ == "__main__":
    run_backfill()