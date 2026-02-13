import pdfplumber
import pandas as pd
import os


def extract_tables_from_pdf(pdf_path):
    """
    Extract all tables from an Invesco factsheet PDF
    """

    all_tables = []

    print(f"Reading PDF → {pdf_path}")

    with pdfplumber.open(pdf_path) as pdf:

        for page_no, page in enumerate(pdf.pages):

            tables = page.extract_tables()

            if tables:

                for table in tables:

                    df = pd.DataFrame(table)

                    # Drop fully empty rows
                    df.dropna(how="all", inplace=True)

                    if not df.empty:
                        df["Page"] = page_no + 1
                        all_tables.append(df)

    return all_tables


def save_tables_to_excel(tables, output_path):
    """
    Save extracted tables into Excel (multiple sheets)
    """

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        for i, table in enumerate(tables):
            sheet_name = f"Table_{i+1}"
            table.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Saved Excel → {output_path}")


def run_parser():

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    pdf_dir = os.path.join(base_dir, "data", "raw_files", "invesco")

    output_dir = os.path.join(base_dir, "data", "processed", "invesco")
    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(pdf_dir):

        if file.endswith(".pdf"):

            pdf_path = os.path.join(pdf_dir, file)

            tables = extract_tables_from_pdf(pdf_path)

            if tables:

                excel_name = file.replace(".pdf", ".xlsx")
                output_path = os.path.join(output_dir, excel_name)

                save_tables_to_excel(tables, output_path)

            else:
                print(f"No tables found → {file}")


if __name__ == "__main__":
    run_parser()
