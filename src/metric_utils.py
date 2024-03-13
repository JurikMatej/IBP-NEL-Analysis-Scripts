import re
import pandas as pd

PATTER_DOMAIN_FROM_URL = r"http[s]?:[\/][\/]([^\/:]+)"


def calculate_total_nel_domains(url_column: pd.Series):
    return len(url_column.apply(
        lambda url: re.search(PATTER_DOMAIN_FROM_URL, url).group(1) if re.match(PATTER_DOMAIN_FROM_URL, url) else None)
     .dropna()
     .drop_duplicates()
     .reset_index(drop=True))
