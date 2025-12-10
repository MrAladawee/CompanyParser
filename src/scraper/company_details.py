import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}


def extract_address(company_url):
    try:
        response = requests.get(company_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        rows = soup.select("table.table.reee_table tr")
        for row in rows:
            th = row.find("th")
            td = row.find("td")
            if th and td and "Адрес одной строкой как в ЕГРЮЛ" in th.text:
                return td.text.strip()

    except Exception:
        return ""

    return ""
