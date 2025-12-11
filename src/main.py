import sys
import csv
import tqdm
import os

from src.scraper.regions_cities import get_region_links, get_city_links
from src.scraper.okved import extract_okved_links, extract_companies
from src.processing.merge_info import merge_company_info
from src.processing.enrich_dadata import enrich_with_dadata

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

# ==========================
# ЭТАП 1 — СБОР ИНН
# ==========================
def step1_collect_regions_cities():
    print("[1/2] Сбор регионов и городов...")
    ensure_data_dir()
    regions = get_region_links()
    all_data = []

    for region in tqdm.tqdm(regions, desc = "Регионы"):
        cities = get_city_links(region["region_url"])
        for city in cities:
            all_data.append({
                "region_name": region['region_name'],
                "region_url": region['region_url'],
                "city_name": city['city_name'],
                "city_url": city['city_url']
            })

    FILE_PATH = os.path.join(DATA_DIR, "regions_and_cities.csv")
    with open(FILE_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["region_name", "region_url", "city_name", "city_url"])
        writer.writeheader()
        writer.writerows(all_data)

    print("Готово: data/regions_and_cities.csv")


def step2_collect_companies():
    print("[2/2] Сбор компаний по ОКВЭД (62/63)...")

    REGIONS_FILE = os.path.join(DATA_DIR, "regions_and_cities.csv")
    with open(REGIONS_FILE, encoding="utf-8") as infile:
        rows = list(csv.DictReader(infile))

    COMPANIES_FILE = os.path.join(DATA_DIR, "it_companies_okved_62_63.csv")
    with open(COMPANIES_FILE, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["region", "city", "company_name", "inn", "link", "okved", "address"])

        for row in tqdm.tqdm(rows, desc="Поиск компаний"):
            city_url = row["city_url"]
            okved_links = extract_okved_links(city_url)

            for okved_code, okved_url in okved_links:
                companies = extract_companies(okved_url)
                for company in companies:
                    writer.writerow([
                        row["region_name"],
                        row["city_name"],
                        company["name"],
                        company["inn"],
                        company["link"],
                        okved_code,
                        company["address"]
                    ])

    print("Готово: data/it_companies_okved_62_63.csv")
    print("\n=== ВАЖНО ===")
    print("Откройте файл it_companies_okved_62_63.csv, возьмите ИНН и соберите companyinfo.xlsx вручную с помощью инструкции на GitHub.")
    print("После этого выполните: python -m src.main process")


# ==========================
# ЭТАП 2 — ОБРАБОТКА + DADATA
# ==========================
def run_processing_pipeline():
    print("Шаг 3: объединение с companyinfo.xlsx…")
    merge_company_info()

    print("Шаг 4: обогащение через DaData…")
    enrich_with_dadata()

    print("ГОТОВО! Финальный файл: data/companies.csv")


# ==========================
# CLI-режим
# ==========================
def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python -m src.main collect   → собрать списки и ИНН")
        print("  python -m src.main process   → обработать Excel + DaData")
        return

    mode = sys.argv[1]

    if mode == "collect":
        step1_collect_regions_cities()
        step2_collect_companies()
    elif mode == "process":
        run_processing_pipeline()
    else:
        print("Неизвестная команда:", mode)
        print("Доступные команды: collect, process")


if __name__ == "__main__":
    main()
