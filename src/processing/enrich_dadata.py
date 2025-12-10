import time
import pandas as pd
from dadata import Dadata


def enrich_with_dadata():
    token = "ВАШ_ТОКЕН"
    dadata = Dadata(token)

    df = pd.read_csv("data/companies.csv")

    addresses = []
    managers = []

    for inn in df["inn"]:
        try:
            result = dadata.find_by_id("party", str(inn), branch_type="MAIN")

            if result:
                data = result[0]["data"]
                address = data.get("address", {}).get("unrestricted_value", "")
                manager = data.get("management", {}).get("name", "")
            else:
                address = ""
                manager = ""

        except Exception:
            address = ""
            manager = ""

        addresses.append(address)
        managers.append(manager)

        time.sleep(1)

    df["address"] = addresses
    df["management"] = managers

    df.to_csv("data/companies.csv", index=False)
