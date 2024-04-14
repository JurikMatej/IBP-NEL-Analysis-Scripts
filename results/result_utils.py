from pathlib import Path
from typing import List

import pandas as pd
from pandas import DataFrame, Series

MONTHS_MAP = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec"
}


def date_to_text_format(date: str) -> str:
    year, month = date.split('-')
    month = MONTHS_MAP[month]

    return f'{month} {year}'


def concat_data_from_files(used_data_files: List[Path]) -> DataFrame:
    result = None
    for data_file in used_data_files:
        data = pd.read_parquet(data_file)

        if result is None and len(data) != 0:
            result = data
        else:
            result = pd.concat([result, data])

    return result


def get_first_or_0(s: Series) -> int:
    if s.size < 1:
        return 0
    return s.iloc[0]
