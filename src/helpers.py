from bs4 import BeautifulSoup
from collections import namedtuple
import requests


TINKOFF_URL = "https://www.tinkoff.ru"
Bond = namedtuple("Bond", "data url isin")


def get_bonds_url(url: str):
    page = requests.get(url)
    if (sc := page.status_code) != 200:
        raise RuntimeError(
            f"не удалось подключиться (status_code = {sc})"
        )
    body = BeautifulSoup(page.content, "html.parser").body
    table = body.find(
        "table", {"data-qa-file": "DataTable"}
    )
    rows = table.tbody.find_all("tr")
    refs = [TINKOFF_URL + tr.a["href"] for tr in rows]
    isin_list = [ref.split("/")[-2] for ref in refs]
    return refs, isin_list
