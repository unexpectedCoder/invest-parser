import pandas as pd

from src.helpers import Bond


def data_postprocessing(bonds: list[Bond]):
    data = pd.DataFrame(bonds)
    data["Текущая доходность"] = \
        0.87 * data["Величина купона"] * data["Количество выплат в год"] / (1.003 * data["Рыночная цена"])
    data["Дней до погашения"] = (
        pd.to_datetime(data["Дата погашения облигации"]) - pd.to_datetime("today")
    ) + pd.Timedelta(days=1)
    d = data["Дней до погашения"]
    d[d.isnull()] = 0
    d = data["Накопленный купонный доход"]
    d[d.isnull()] = 0
    data["% НКД от купона"] = \
        data["Накопленный купонный доход"] / data["Величина купона"]
    return data
