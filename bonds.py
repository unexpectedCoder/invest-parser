from bs4 import BeautifulSoup
from datetime import date
from enum import Enum
import pandas as pd
import requests
import time


class InvestURL(Enum):
    TINKOFF = "https://www.tinkoff.ru"


def get_bonds(invest_url: str, table_url: str):
    urls, isin_list = _get_bonds_url(invest_url, table_url)
    bonds = []
    for url, isin in zip(urls, isin_list):
        bond = _get_bond(url, isin)
        if isinstance(bond, str):
            bond = _get_bond(url, isin)
        if isinstance(bond, dict):
            bonds.append(bond)
    return pd.DataFrame(bonds)


def _get_bonds_url(invest_url: str, table_url: str):
    page = requests.get(table_url)
    if (sc := page.status_code) != 200:
        raise RuntimeError(
            f"не удалось подключиться (status_code = {sc})"
        )
    body = BeautifulSoup(page.content, "html.parser").body
    table = body.find(
        "table", {"data-qa-file": "DataTable"}
    )
    rows = table.tbody.find_all("tr")
    refs = [invest_url + tr.a["href"] for tr in rows]
    isin_list = [ref.split("/")[-2] for ref in refs]
    return refs, isin_list


def _get_bond(url: str, isin: str):
    page = requests.get(url)
    if (sc := page.status_code) != 200:
        raise RuntimeError(
            f"не удалось подключиться (status_code = {sc})"
        )
    body = BeautifulSoup(page.content, "html.parser").body
    tables = body.find_all("table", {"data-qa-file": "Table"})
    cells = []
    for table in tables:
        cells.extend(table.find_all("td", {"data-qa-file": "TableCell"}))
    data = {c1.text.replace("\xa0", ""): c2.text.replace("\xa0", "").replace(",", ".").replace(" ", "")
            for c1, c2 in zip(cells[::2], cells[1::2])}
    for k in data:
        if data[k][-1] == "₽":
            data[k] = float(data[k][:-1])
        elif data[k][-1] == "%":
            if not data[k][0].isdigit():
                data[k] = -float(data[k][1:-1]) / 100
            else:
                data[k] = float(data[k][:-1]) / 100
        elif "дата" in k.lower():
            d = data[k].split(".")[::-1]
            for i in range(len(d)):
                d[i] = int(d[i])
            data[k] = date(*d)
        elif "количество" in k.lower():
            data[k] = int(data[k])
    price_table = body.find("div", {"data-qa-file": "SecurityPriceDetails"})
    try:
        price = price_table.find("span")
    except AttributeError:
        return url
    if price.text[0].isdigit():
        price = float(
            price.text.replace("\xa0", "").replace(",", ".").replace(" ", "")[:-1]
        )
    else:
        price = 0.
    data["Рыночная цена"] = price
    data["ISIN"] = isin
    return data


def calc_current_yield(bonds: pd.DataFrame):
    return bonds["Величина купона"] * bonds["Количество выплат в год"] / bonds["Рыночная цена"]


def calc_days(bonds: pd.DataFrame):
    return (pd.to_datetime(bonds["Дата погашения облигации"]) - pd.to_datetime("today")) + pd.Timedelta(days=1)


if __name__ == "__main__":
    import os

    print("Парсинг ОФЗ...")
    start_end = (0, 99), (100, 199), (200, 299), (300, 399), (400, 499)
    bonds_ = []
    for start, end in start_end:
        url_ = f"https://www.tinkoff.ru/invest/bonds/?start={start}&end={end}&country=Russian&orderType=Desc&sortType=ByYieldToClient&rate=2"
        bonds_.append(get_bonds(InvestURL.TINKOFF.value, url_))
        time.sleep(1.2)
    excel_name = "all_bonds.xlsx"
    bonds_ = pd.concat(bonds_)
    bonds_["Текущая доходность"] = calc_current_yield(bonds_)
    bonds_["Дней до погашения"] = calc_days(bonds_)
    bonds_.sort_values(["Текущая доходность"], ascending=False, inplace=True)
    bonds_.to_excel(excel_name)
    print(f"Результат сохранён в '{os.path.abspath(excel_name)}'")
