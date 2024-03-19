import re
import pandas as pd

PATTERN_DOMAIN_FROM_URL = r"http[s]?:[\/][\/]([^\/:]+)"


def calculate_total_nel_domains(url_column: pd.Series) -> int:
    return len(url_column.apply(_extract_domain_from_url)
               .dropna()
               .drop_duplicates()
               .reset_index(drop=True))


def get_rt_collectors_for_unique_domains_with_nel(nel_resources_with_rt_collectors_df: pd.DataFrame) -> pd.Series:
    tmp_df = nel_resources_with_rt_collectors_df.copy()

    tmp_df['domain'] = tmp_df['url'].map(_extract_domain_from_url)
    tmp_df = tmp_df.dropna(subset=['domain'])

    # IMPORTANT NOTE: taking the first found rt_collectors set for a unique domain here
    # (assuming that the domain has consistent NEL setup deployed - all resources share the same primary NEL collector)
    return tmp_df.groupby(['domain']).agg({
        "rt_collectors": "first"
    }).reset_index()


def _extract_domain_from_url(url):
    return re.search(PATTERN_DOMAIN_FROM_URL, url).group(1) if re.match(PATTERN_DOMAIN_FROM_URL, url) else None
