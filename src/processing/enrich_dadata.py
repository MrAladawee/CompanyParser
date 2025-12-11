import time
import os
import pandas as pd
import tqdm
from dadata import Dadata
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(SRC_DIR, "data")

load_dotenv()
DADATA_TOKEN = os.getenv("DADATA_TOKEN")

def enrich_with_dadata():
    dadata = Dadata(DADATA_TOKEN)

    input_file = os.path.join(DATA_DIR, "companies.csv")
    output_file = os.path.join(DATA_DIR, "companies.csv")

    df = pd.read_csv(input_file)

    addresses = []
    managers = []
    with tqdm.tqdm(total=len(df), desc="Обогащение данных DaData") as pbar:
        for inn in df["inn"]:
            try:
                result = dadata.find_by_id("party", str(inn), branch_type="MAIN")
                if result:
                    data = result[0]["data"]
                    address = data.get("address", {}).get("unrestricted_value", "")
                    manager = data.get("management", {}).get("name", "")
                else:
                    address = "1"
                    manager = "1"
            except Exception as e:
                print(f"Ошибка при обработке ИНН {inn}: {e}")
                address = ""
                manager = ""

            addresses.append(address)
            managers.append(manager)

            time.sleep(1)
            pbar.update(1)

        df["address"] = addresses
        df["management"] = managers

        df.to_csv(output_file, index=False)
