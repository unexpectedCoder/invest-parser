from aiohttp import ClientSession
from bs4 import BeautifulSoup
from datetime import date
from multiprocessing.pool import Pool
import asyncio
import logging
import os
import pandas as pd

from src.helpers import TINKOFF_URL, Bond, get_bonds_url


async def parse_bonds(bonds_url: str, pool: Pool):
    urls, isins = get_bonds_url(bonds_url)
    async with ClientSession() as session:
        pending = [
            asyncio.create_task(_get_bond(session, url, isin))
            for url, isin in zip(urls, isins)
        ]
        results = []
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for done_task in done:
                if done_task.exception() is None:
                    results.append(
                        pool.apply_async(_process_bond, (done_task.result(),))
                    )
                else:
                    logging.error("Ошибка", exc_info=done_task.exception())
        return [r.get() for r in results]


async def _get_bond(session: ClientSession, url: str, isin: str):
    async with session.get(url) as resp:
        page = await resp.read()
        body = BeautifulSoup(page, "html.parser").body
        tables = body.find_all("table", {"data-qa-file": "Table"})
        cells = []
        for table in tables:
            cells.extend(table.find_all("td", {"data-qa-file": "TableCell"}))
        data = {
            c1.text.replace("\xa0", ""): c2.text.replace("\xa0", "").replace(",", ".").replace(" ", "")
            for c1, c2 in zip(cells[::2], cells[1::2])
        }
        price_table = body.find("div", {"data-qa-file": "SecurityPriceDetails"})
        price = price_table.find("span")
        if price.text[0].isdigit():
            price = float(
                price.text.replace("\xa0", "").replace(",", ".").replace(" ", "")[:-1]
            )
        else:
            price = 0.
        data["Рыночная цена"] = price
        data["ISIN"] = isin
        return Bond(data, url, isin)


def _process_bond(bond: Bond):
    data = bond.data
    for k in data:
        if isinstance(data[k], float):
            continue
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
    return data


def calc_current_yield(bonds: pd.DataFrame):
    return \
        bonds["Величина купона"] * \
        bonds["Количество выплат в год"] / \
        bonds["Рыночная цена"]


def calc_days(bonds: pd.DataFrame):
    return (
        pd.to_datetime(bonds["Дата погашения облигации"]) - pd.to_datetime("today")
    ) + pd.Timedelta(days=1)


if __name__ == "__main__":
    _url = TINKOFF_URL + \
        f"/invest/bonds/?start={0}&end={450}&country=Russian&orderType=Desc&sortType=ByYieldToClient&rate=2"
    with Pool() as _pool:
        loop = asyncio.get_event_loop()
        _bonds = loop.run_until_complete(parse_bonds(_url, _pool))
        loop.close()
        _data = pd.DataFrame(_bonds)
        _data["Текущая доходность"] = calc_current_yield(_data)
        _data["Дней до погашения"] = calc_days(_data)
        _data.sort_values(["Текущая доходность"], ascending=False, inplace=True)
        _save_file = "all_bonds.xlsx"
        _data.to_excel(_save_file)
        print(f"Результат сохранён в '{os.path.abspath(_save_file)}'")
