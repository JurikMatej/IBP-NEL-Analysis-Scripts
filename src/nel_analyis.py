import pandas as pd

import src.metric_utils as metric_utils


def yearly_nel_deployment_next(month_data: pd.DataFrame, year: str, month: str):
    if len(month_data) == 0:
        return pd.DataFrame()

    total_domains = month_data['total_crawled_domains'][0]
    total_nel_domains = metric_utils.calculate_total_nel_domains(month_data['url'].copy())
    nel_percentage = total_nel_domains / total_domains * 100

    result = pd.DataFrame({
        "date": [f"{year}-{month}"],
        "domains": [total_domains],
        "nel": [total_nel_domains],
        "nel_percentage": [nel_percentage]
    }).reset_index(drop=True)

    return result


def produce_output_yearly_nel_deployment(aggregated_metric: pd.DataFrame):
    print(aggregated_metric)


def update_nel_collector_providers(month_data: pd.DataFrame, year: str, month: str):
    # TODO if this metric needs to be aggregated by YEAR (not month), changes to the aggregation process need to be made
    rt_collectors_occurrences = month_data['rt_collectors'].explode()
    rt_collectors_share_distribution = rt_collectors_occurrences.value_counts()

    top_4_providers = rt_collectors_share_distribution.sort_values(ascending=False).head(n=4)
    top_4_providers_share = top_4_providers / len(rt_collectors_occurrences) * 100

    rt_collectors_unique = rt_collectors_occurrences.unique()

    result = pd.DataFrame({
        "date": [f"{year}-{month}"] * len(top_4_providers),
        "count": [len(rt_collectors_unique)] * len(top_4_providers),
        "top_4_providers": top_4_providers.index,
        "share_%": top_4_providers_share
    }).reset_index(drop=True)

    return result


def produce_output_nel_collector_providers(aggregated_metric: pd.DataFrame):
    output = aggregated_metric.groupby(["date", "count", "top_4_providers", "share_%"]).all()
    print(output)


# TODO others
