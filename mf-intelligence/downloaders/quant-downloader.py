import requests
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta  


BASE_URL = "https://quantmutual.com/Admin/disclouser/"

## To parse the fund names
def parse_fund_names(json_data):

    html_data = json_data['d']

    links = re.findall(r"Admin/disclouser/.*?\.xlsx", html_data)

    fund_names = set()   

    for link in links:

        file_name = link.split("/")[-1]

        parts = file_name.replace(".xlsx", "").split("_")

        fund_name = "_".join(parts[:-2])

        fund_names.add(fund_name)

    return sorted(fund_names)


def generate_months(n_months=12):
    """
    Generate last 12 months in Mon_Year format
    Example: Jan_2026
    """

    months = []

    today = datetime.today()

    ## Standardize the months as per the Quant API
    std_mon = {"01":"Jan", "02":"Feb", "03":"Mar", "04":"Apr", "05":"May", "06":"Jun", "07":"July", "08":"Aug", "09":"Sep", "10":"Oct", "11":"Nov", "12":"Dec"}
        

    for i in range(n_months):
        date = today - relativedelta(months=i)

        mon = date.strftime("%m")  # 01, 02, 03,...12
        year = date.strftime("%Y") # 2026, 2025, .....
        months.append(std_mon[mon]+'_'+year) # Jan_2026
    return months


def download_file(file_url, save_dir):

    file_name = file_url.split("/")[-1]
    save_path = os.path.join(save_dir, file_name)

    if os.path.exists(save_path):
        print("Already exists:", file_name)
        return

    print("Downloading:", file_name)

    try:
        response = requests.get(file_url, timeout=30)

        if response.status_code == 200:

            with open(save_path, "wb") as f:
                f.write(response.content)

            print("Saved ->", save_path)

        else:
            print("Not Available:", file_name)

    except Exception as e:
        print("Error:", file_name, "|", e)



def run_backfill(fund_name):

    # Project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    save_dir = os.path.join(base_dir, "data", "raw_files", "quant", fund_name)
    os.makedirs(save_dir, exist_ok=True)

    months = generate_months()

    print(f"\nDownloading last {len(months)} months...\n")

    for m in months:

        file_name = f"{fund_name}_{m}.xlsx"
        file_url = BASE_URL + file_name

        download_file(file_url, save_dir)

    print(f"\nBackfill download completed for {fund_name}")


if __name__ == "__main__":

    json_data = {"d": "\u003cul\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Aggressive_Hybrid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Aggressive Hybrid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_and_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large \u0026 Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Small_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Small Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Infrastructure_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Infrastructure Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Focused_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Focused Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Liquid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Liquid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Flexi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Flexi cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ELSS_Tax_Saver_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ELSS Tax Saver Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ESG_Integration_Strategy_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ESG Integration Strategy Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Quantamental_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Quantamental Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Value_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Value Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large cap fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Gilt_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Gilt Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Overnight_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Overnight Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Dynamic_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Dynamic Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Business_Cycle_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Business Cycle Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_BFSI_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant BFSI Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Healthcare_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Healthcare Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Manufacturing_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Manufacturing Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Teck_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Teck Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Momentum_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Momentum Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Commodities_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Commodities Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Consumption_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Consumption Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_PSU_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant PSU Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Arbitrage_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Arbitrage Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Equity_Savings_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Equity Savings Fund\u003c/a\u003e\u003c/li\u003e\u003c/ul\u003e"
    }

    fund_names = parse_fund_names(json_data)

    for fund_name in fund_names:
        run_backfill(fund_name)