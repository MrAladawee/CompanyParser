import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(SRC_DIR, "data")

def merge_company_info():

    companies_csv = os.path.join(DATA_DIR, "it_companies_okved_62_63.csv")
    info_xlsx = os.path.join(DATA_DIR, "companyinfo.xlsx")
    output_no_filter = os.path.join(DATA_DIR, "companies_noFilter.csv")
    output_final = os.path.join(DATA_DIR, "companies.csv")

    if not os.path.exists(companies_csv):
        raise FileNotFoundError(f"Файл не найден: {companies_csv}")

    if not os.path.exists(info_xlsx):
        raise FileNotFoundError(f"Файл не найден: {info_xlsx}")

    main_df = pd.read_csv(companies_csv)
    info_df = pd.read_excel(info_xlsx)

    main_df["inn"] = main_df["inn"].astype(str).str.strip()
    info_df["ИНН"] = info_df["ИНН"].astype(str).str.strip()

    merged = pd.merge(main_df, info_df, left_on="inn", right_on="ИНН", how="left")

    result = pd.DataFrame({
        "inn": merged["inn"],
        "name": merged["company_name"],
        "employees": pd.to_numeric(
            merged["Среднесписочная численность работников за предшествующий календарный год"],
            errors="coerce"
        ),
        "okved_main": merged["okved"],
        "region": merged["region"].str.split("\n").str[0].str.strip()
                   + ", " + merged["city"].astype(str).str.strip(),
        "source": "custom_parser"
    })

    result['employees'] = result['employees'].fillna(0).astype(int)

    result.to_csv(output_no_filter, index=False)

    result_filtered = result[result['employees'] >= 100]
    result_filtered.to_csv(output_final, index=False)

    print(f"Создан файл: {output_no_filter}")
    print(f"Создан файл: {output_final}")
