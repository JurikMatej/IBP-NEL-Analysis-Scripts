import io

import numpy as np
import pandas as pd

import src.metric_utils as metric_utils
from src import psl_utils


def update_monthly_nel_deployment(month_data: pd.DataFrame, year: str, month: str):
    if len(month_data) == 0:
        # TODO no data = no record of total crawled domains
        return pd.DataFrame()

    total_domains = month_data['total_crawled_domains'][0]
    total_nel_domains = month_data['total_crawled_domains_with_correct_nel'][0]
    nel_percentage = total_nel_domains / total_domains * 100

    result = pd.DataFrame({
        "date": [f"{year}-{month}"],
        "domains": [total_domains],
        "nel": [total_nel_domains],
        "nel_percentage": [nel_percentage]
    }).reset_index(drop=True)

    return result


def produce_output_yearly_nel_deployment(aggregated_metric: pd.DataFrame):
    aggregated_metric['domains'] = aggregated_metric['domains'].astype(int)
    aggregated_metric['nel'] = aggregated_metric['nel'].astype(int)
    aggregated_metric.to_html("out/monthly_nel_deployment.html")


def update_nel_collector_providers(month_data: pd.DataFrame, year: str, month: str, used_psl: io.StringIO):
    data_by_nel_domains = metric_utils.get_registrable_collectors_for_unique_domains_with_nel(
        month_data[['url_domain', 'rt_collectors_registrable']]
    )

    rt_registrable_domain_collector_occurrences = data_by_nel_domains['rt_collectors_registrable'].map(
        lambda collector_list: collector_list[0] if len(collector_list) > 0 else None)

    rt_registrable_domain_collector_occurrences.dropna(inplace=True)

    rt_collectors_share_distribution = rt_registrable_domain_collector_occurrences.value_counts()

    top_providers = rt_collectors_share_distribution.sort_values(ascending=False)
    top_providers_share = top_providers / len(rt_registrable_domain_collector_occurrences) * 100

    rt_collectors_unique = rt_registrable_domain_collector_occurrences.unique()

    result = pd.DataFrame({
        "date": [f"{year}-{month}"] * len(top_providers),
        "count": [len(rt_collectors_unique)] * len(top_providers),
        "top_providers": top_providers.index,
        "share_%": top_providers_share
    })

    return result


def produce_output_nel_collector_providers(aggregated_metric: pd.DataFrame):
    aggregated_metric['count'] = aggregated_metric['count'].astype(int)
    aggregated_metric['share_%'] = aggregated_metric['share_%'].astype(dtype=np.float32)

    output = (aggregated_metric
              .groupby(["date", "count", "top_providers", "share_%"])
              .all()
              .sort_values(by=["date", "share_%"], ascending=[True, False])
              )

    output.to_html("out/nel_collector_providers.html")

# TODO others
