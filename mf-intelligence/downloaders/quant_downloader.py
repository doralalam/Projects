import requests
import os


def download_factsheet(file_url, file_name):
    """
    Download portfolio disclosure from the given URL
    """
    # Current location
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # join the location
    save_dir = os.path.join(BASE_DIR, "data", "raw_files")
    os.makedirs(save_dir, exist_ok=True)

    save_path = os.path.join(save_dir, file_name)

    print("Downloading:", file_url)

    response = requests.get(file_url)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)

        print("Saved →", save_path)

    else:
        print("Failed. Status code:", response.status_code)


if __name__ == "__main__":

    url = "https://quantmutual.com/Admin/disclouser/quant_Small_Cap_Fund_Jan_2026.xlsx"

    file_name = "Quant_SmallCap_Jan_2026.pdf"

    download_factsheet(url, file_name)
