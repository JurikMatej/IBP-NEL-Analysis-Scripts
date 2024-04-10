import pandas as pd
from pandas import DataFrame


def load_tranco_list(year: str, month: str) -> DataFrame | None:
    popular_domain_name_column_idxs = [0, 1]

    try:
        return pd.read_csv(f"resources/tranco/tranco_{year}_{month}.csv",
                           header=None,
                           usecols=popular_domain_name_column_idxs,
                           names=['order', 'popular_domain_name'])
    except FileNotFoundError:
        # No tranco list = no popular domain results
        return None
