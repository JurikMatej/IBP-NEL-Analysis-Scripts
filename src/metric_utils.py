import pandas as pd


def get_registrable_collectors_for_unique_domains_with_nel(nel_resources_with_rt_collectors_df: pd.DataFrame) -> pd.Series:
    tmp_df = nel_resources_with_rt_collectors_df.copy()

    tmp_df = tmp_df.dropna(subset=['url_domain'])

    # IMPORTANT NOTE: taking the first found rt_collectors set for a unique domain here
    # (assuming that the domain has consistent NEL setup deployed - all resources share the same primary NEL collector)
    return tmp_df.groupby(['url_domain']).agg({
        "rt_collectors_registrable": "first"
    }).reset_index()

