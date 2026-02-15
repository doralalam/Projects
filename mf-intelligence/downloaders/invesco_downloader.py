import requests
import os


BASE_URL = "https://www.invescomutualfund.com/docs/default-source/factsheet/"

FILE_NAME = "invesco-mf-factsheet---january-2026.pdf?sfvrsn=c5829ac2_0"


def download_file(file_url, save_path):

    print("Downloading:", file_url)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf"
    }

    r = requests.get(file_url, headers=headers)

    content_type = r.headers.get("Content-Type", "")

    if "pdf" not in content_type:
        print("Not a valid PDF →", content_type)
        return

    with open(save_path, "wb") as f:
        f.write(r.content)

    print("Saved ->", save_path)


def run_invesco_download():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(
        base_dir,
        "data",
        "raw_files",
        "invesco"
    )

    os.makedirs(save_dir, exist_ok=True)

    file_url = BASE_URL + FILE_NAME

    # Save with real filename
    clean_name = FILE_NAME.split("?")[0]   # remove token
    save_path = os.path.join(save_dir, clean_name)

    download_file(file_url, save_path)

    print("\nInvesco download completed")


if __name__ == "__main__":
    run_invesco_download()
