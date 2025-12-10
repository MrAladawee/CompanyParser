import pandas as pd


def merge_company_info():
    main_df = pd.read_csv("data/it_companies_okved_62_63.csv")
    info_df = pd.read_excel("data/companyinfo.xlsx")

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

    result.to_csv("data/companies_noFilter.csv", index=False)

    result_filtered = result[result['employees'] >= 100]
    result_filtered.to_csv("data/companies.csv", index=False)
