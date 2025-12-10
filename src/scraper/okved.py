import requests
from bs4 import BeautifulSoup
from .company_details import extract_address

BASE_URL = "https://vypiska-nalog.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def extract_okved_links(city_url):
    response = requests.get(city_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []
    table = soup.find("table", class_="reestr_okved_item")

    if not table:
        return links

    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) >= 2:
            code = tds[0].text.strip()
            link_tag = tds[1].find("a")

            if link_tag and code.startswith(("62", "63")):
                links.append((code, BASE_URL + link_tag.get("href")))

    return links


def extract_companies(okved_url):
    response = requests.get(okved_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    companies = []
    items = soup.find_all("div", class_="col-md-6 reestr_region_item")

    for item in items:
        name_tag = item.find("a")
        inn_tag = item.find("p")
        status_tag = item.find("span", class_="text-success")

        if not (name_tag and inn_tag and status_tag):
            continue

        if "Действующая" not in status_tag.text:
            continue

        name = name_tag.text.strip()
        inn = inn_tag.text.replace("ИНН:", "").strip()
        link = BASE_URL + name_tag.get("href")

        address = extract_address(link)

        companies.append({
            "name": name,
            "inn": inn,
            "link": link,
            "address": address
        })

    return companies
