import requests
import os

SAVE_DIR = "edelweiss_monthly"
os.makedirs(SAVE_DIR, exist_ok=True)

API = "https://api.edelweissmf.com/edelweissmf/api/v1/third-party/getSingleStatutory?menuid=2"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.edelweissmf.com/statutory/portfolio-of-schemes",
    "Origin": "https://www.edelweissmf.com",
}

print("🔎 Fetching disclosure metadata...")

r = requests.get(API, headers=headers)

if r.status_code != 200:
    print("❌ Blocked:", r.status_code)
    exit()

data = r.json()

downloads = []

# Explore JSON structure
for section in data.get("data", []):
    
    title = section.get("title", "")

    if "Monthly Portfolio" not in title:
        continue

    for file in section.get("files", []):
        
        name = file.get("fileName")
        url = file.get("fileUrl")

        if url and url.endswith(".xlsx"):
            downloads.append((name, url))

print(f"Found {len(downloads)} monthly XLSX files")

# Download
for name, url in downloads:

    path = os.path.join(SAVE_DIR, name)

    if os.path.exists(path):
        continue

    print("⬇️", name)

    try:
        res = requests.get(url, headers=headers, timeout=30)

        if res.status_code == 200:
            with open(path, "wb") as f:
                f.write(res.content)

    except Exception as e:
        print("Failed:", e)
