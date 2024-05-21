from bs4 import BeautifulSoup
from datetime import date
import requests
import time

from src.helpers import TINKOFF_URL, get_bonds_url


def get_bonds(bonds_url: str):
    urls, isin_list = get_bonds_url(bonds_url)
    bonds = []
    for url, isin in zip(urls, isin_list):
        bond = _get_bond(url, isin)
        if isinstance(bond, str):
            bond = _get_bond(url, isin)
        if isinstance(bond, dict):
            bonds.append(bond)
    return bonds


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


if __name__ == "__main__":
    from termcolor import cprint
    from tqdm import tqdm
    import os
    from src.calculator import data_postprocessing
    from src.helpers import create_cmd_parser

    args = create_cmd_parser().parse_args()
    url = TINKOFF_URL + \
        f"/invest/bonds/" \
        f"?start={args.start}" \
        f"&end={args.end}" \
        f"&country=Russian&orderType=Desc&sortType=ByYieldToClient" \
        f"&rate={args.rate}"
    
    print(f"Парсинг {url}...")
    
    start_end = (0, 99), (100, 199), (200, 299), (300, 399), (400, 499)
    bonds = []
    for start, end in tqdm(start_end, "Парсинг ОФЗ"):
        bonds.extend(get_bonds(url))
        time.sleep(1.5)

    data = data_postprocessing(bonds)
    data.sort_values(["Текущая доходность"], ascending=False, inplace=True)

    save_dir = "results"
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)
    save_file: str = os.path.join(save_dir, args.output)
    if save_file.endswith(".xlsx") or save_file.endswith(".xls"):
        data.to_excel(save_file)
    elif save_file.endswith(".csv"):
        data.to_csv(save_file)
    else:
        raise ValueError("неподдерживаемый формат выходного файла")

    print("Готово!")
    cprint(f"Результат сохранён в '{os.path.abspath(save_file)}'", "green")
