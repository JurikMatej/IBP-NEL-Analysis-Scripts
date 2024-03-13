import pandas as pd
from .metric_utils import calculate_total_nel_domains


def yearly_nel_deployment_next(month_data: pd.DataFrame, year: str, month: str):
    if len(month_data) == 0:
        return pd.DataFrame()

    total_domains = month_data['total_crawled_domains'][0]
    total_nel_domains = calculate_total_nel_domains(month_data['url'].copy())
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


def update_nel_collector_providers(month_data: pd.DataFrame, metric_aggregate: pd.DataFrame, year: str, month: str):
    pass


def produce_output_nel_collector_providers(aggregated_metric: pd.DataFrame):
    pass


# TODO others