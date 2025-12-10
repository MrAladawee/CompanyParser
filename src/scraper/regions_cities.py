import time
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://vypiska-nalog.com"
START_URL = f"{BASE_URL}/reestr/ul"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def get_region_links():
    response = requests.get(START_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    region_blocks = soup.select("div.reestr_region_item a")

    regions = []
    for a in region_blocks:
        href = a.get("href")
        name = a.text.strip()
        if href and href.startswith("/reestr/ul/"):
            regions.append({
                "region_name": name,
                "region_url": BASE_URL + href
            })
    return regions


def get_city_links(region_url):
    time.sleep(1)
    response = requests.get(region_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    city_blocks = soup.select("div.reestr_city_item a")

    cities = []
    for a in city_blocks:
        href = a.get("href")
        name = a.text.strip()
        if href:
            cities.append({
                "city_name": name,
                "city_url": BASE_URL + href
            })
    return cities
