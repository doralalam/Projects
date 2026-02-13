import requests
import os
import re


def download_factsheet(base_url, links):
    """
    Download portfolio disclosure from the given URL
    """
    # Current location
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # join the location
    save_dir = os.path.join(
        base_dir, 
        "data", 
        "raw_files", 
        "quant"
        )
    
    os.makedirs(save_dir, exist_ok=True)

    for link in links:

        file_url = base_url+link

        print("Downloading:", file_url)

        response = requests.get(file_url)

        save_path = os.path.join(save_dir, file_url.split("/")[-1])

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(response.content)

            print("Saved -> ", save_path)

        else:
            print("Failed. Status code:", response.status_code)


if __name__ == "__main__":

    base_url = "https://quantmutual.com/"

    json_data = {
        ## enter the json string here
    "d": "\u003cul\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Aggressive_Hybrid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Aggressive Hybrid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Multi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Multi Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_and_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large \u0026 Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Small_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Small Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Infrastructure_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Infrastructure Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Focused_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Focused Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Liquid_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Liquid Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Mid_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Mid Cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Flexi_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Flexi cap Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ELSS_Tax_Saver_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ELSS Tax Saver Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_ESG_Integration_Strategy_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant ESG Integration Strategy Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Quantamental_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Quantamental Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Value_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Value Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Large_Cap_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Large cap fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Gilt_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Gilt Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Overnight_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Overnight Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Dynamic_Asset_Allocation_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Dynamic Asset Allocation Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Business_Cycle_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Business Cycle Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_BFSI_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant BFSI Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Healthcare_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Healthcare Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Manufacturing_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Manufacturing Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Teck_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Teck Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Momentum_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Momentum Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Commodities_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Commodities Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Consumption_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Consumption Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_PSU_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant PSU Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Arbitrage_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Arbitrage Fund\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u0026#9658;\u003ca href=\u0027/Admin/disclouser/quant_Equity_Savings_Fund_Jan_2026.xlsx\u0027 target=\u0027_blank\u0027\u003equant Equity Savings Fund\u003c/a\u003e\u003c/li\u003e\u003c/ul\u003e"
    }

    html_data = json_data['d']

    links = re.findall(r"Admin/disclouser/.*?\.xlsx", html_data)

    ## To remove duplicates
    links = list(set(links))

    download_factsheet(base_url, links)




