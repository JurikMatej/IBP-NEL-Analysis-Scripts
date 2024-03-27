import gc
from io import StringIO
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame, Series
from pathlib import Path

from src import psl_utils

# METRIC LEGEND: (see docs/data-contract)
#   cX = custom metric number X
#   bX = base metric number X
#   cX (bY) = custom metric number X - fulfills Y from base metric b

# TODO PREPARE DATA FOR: c6, c8


def update_nel_deployment(month_data_file: Path, year: str, month: str) -> DataFrame:
    """PREPARES DATA FOR: c2 (b1)"""

    # Read only the first row of the month data file (necessary data is already precomputed)
    parquet = pq.ParquetFile(month_data_file)
    first_row = next(parquet.iter_batches(batch_size=1,
                                          columns=['total_crawled_domains', 'total_crawled_domains_with_correct_nel']))
    data = pa.Table.from_batches([first_row]).to_pandas()

    if len(data) == 0:
        # TODO no data = no record of total crawled domains
        return DataFrame()

    total_domains = data['total_crawled_domains'][0]
    total_nel_domains = data['total_crawled_domains_with_correct_nel'][0]
    nel_percentage = np.uint32(total_nel_domains) / np.uint32(total_domains) * 100

    result = DataFrame({
        "date": [f"{year}-{month}"],
        "domains": [total_domains],
        "nel": [total_nel_domains],
        "nel_percentage": [nel_percentage]
    }).reset_index(drop=True)

    del data
    gc.collect()

    return result


def produce_output_yearly_nel_deployment(aggregated_metric: DataFrame):
    aggregated_metric['domains'] = aggregated_metric['domains'].astype(int)
    aggregated_metric['nel'] = aggregated_metric['nel'].astype(int)
    aggregated_metric.to_html("out/nel_deployment.html")


def update_nel_collector_provider_usage(month_data_file: Path, aggregated_providers: Series, year: str, month: str,
                                        used_psl: StringIO) -> DataFrame:
    """PREPARES DATA FOR: c1, c2 (b2 & b3), c4, c5"""

    data = pd.read_parquet(month_data_file,
                           columns=['url_domain', 'rt_collectors_registrable'])

    collectors_per_url_domain = data.groupby(['url_domain'], observed=False).first()

    total_url_domains = len(collectors_per_url_domain)

    all_providers_so_far = Series(np.append(aggregated_providers.values,
                                            collectors_per_url_domain['rt_collectors_registrable'].explode().unique()
                                            )).dropna().unique()
    result = DataFrame({
        "date": [f"{year}-{month}"] * len(all_providers_so_far),
        "providers": all_providers_so_far
    })

    collectors_per_url_domain['primary_collectors'] = collectors_per_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[0] if len(collectors) > 0 else None
    )
    collectors_per_url_domain['secondary_collectors'] = collectors_per_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[1] if len(collectors) > 1 else None
    )
    collectors_per_url_domain['fallback_collectors'] = collectors_per_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[2:] if len(collectors) > 2 else None
    )

    collectors_per_url_domain = collectors_per_url_domain.drop(columns=['rt_collectors_registrable'])

    collectors_per_url_domain.dropna(inplace=True, subset=['primary_collectors'])
    collectors_per_url_domain.reset_index(inplace=True)

    primary_collector_usage = (collectors_per_url_domain
                               .groupby(['primary_collectors'])
                               .agg(as_primary=('url_domain', 'count')))
    primary_collector_usage.reset_index(inplace=True)

    secondary_collector_usage = (collectors_per_url_domain
                                 .groupby(['secondary_collectors'])
                                 .agg(as_secondary=('url_domain', 'count')))
    secondary_collector_usage.reset_index(inplace=True)

    fallback_collectors_per_url_domain = collectors_per_url_domain[['url_domain', 'fallback_collectors']]
    fallback_collector_usage = (fallback_collectors_per_url_domain
                                .explode(['fallback_collectors'])
                                .groupby(['fallback_collectors'])
                                .agg(among_fallback=('url_domain', 'count')))

    result = result.merge(primary_collector_usage, how='left', left_on="providers", right_on="primary_collectors")
    result.drop(columns=['primary_collectors'], inplace=True)
    result['share_as_primary'] = result['as_primary'] / total_url_domains * 100

    result = result.merge(secondary_collector_usage, how='left', left_on="providers", right_on="secondary_collectors")
    result.drop(columns=['secondary_collectors'], inplace=True)
    result['share_as_secondary'] = result['as_secondary'] / result['as_secondary'].sum() * 100

    result = result.merge(fallback_collector_usage, how='left', left_on="providers", right_on="fallback_collectors")

    result['as_primary'] = result['as_primary'].fillna(0)
    result['share_as_primary'] = result['share_as_primary'].fillna(0)

    result['as_secondary'] = result['as_secondary'].fillna(0)
    result['share_as_secondary'] = result['share_as_secondary'].fillna(0)

    result['among_fallback'] = result['among_fallback'].fillna(0)

    del data
    gc.collect()

    return result.reset_index().drop(columns=['index'])


def produce_output_nel_collector_provider_usage(aggregated_metric: DataFrame):
    aggregated_metric.to_html("out/nel_collector_provider_usage.html")


def update_nel_config(input_file: Path, year: str, month: str, used_psl: StringIO) -> Dict[str, DataFrame]:
    """
    PREPARES DATA FOR: c7

    IMPL NOTE: I deliberately avoid loading all the data and making copies of it here to trade computing time for RAM.
    """
    #
    # failure_fraction
    #
    data = pd.read_parquet(input_file, columns=['url_domain', 'nel_failure_fraction'])
    ff_data = DataFrame({
        "date": [f"{year}-{month}"] * len(data),
        "url_domain": data['url_domain'],
        "nel_failure_fraction": data['nel_failure_fraction']
    })
    del data
    gc.collect()

    ff_data = ff_data.groupby(['date', 'url_domain'], as_index=False, observed=False).first()
    ff_data_length = len(ff_data)
    ff_data = ff_data.groupby(['date', 'nel_failure_fraction'], as_index=False, observed=False).agg(
        domain_count=("url_domain", "count"))
    ff_data['domain_percent'] = ff_data['domain_count'] / ff_data_length * 100

    #
    # success_fraction
    #
    data = pd.read_parquet(input_file, columns=['url_domain', 'nel_success_fraction'])
    sf_data = DataFrame({
        "date": [f"{year}-{month}"] * len(data),
        "url_domain": data['url_domain'],
        "nel_success_fraction": data['nel_success_fraction']
    })
    del data
    gc.collect()

    sf_data = sf_data.groupby(['date', 'url_domain'], as_index=False, observed=False).first()
    sf_data_length = len(sf_data)
    sf_data = sf_data.groupby(['date', 'nel_success_fraction'], as_index=False, observed=False).agg(
        domain_count=("url_domain", "count"))
    sf_data['domain_percent'] = sf_data['domain_count'] / sf_data_length * 100

    #
    # include_subdomains
    #
    data = pd.read_parquet(input_file, columns=['url_domain', 'nel_include_subdomains'])
    is_data = DataFrame({
        "date": [f"{year}-{month}"] * len(data),
        "url_domain": data['url_domain'],
        "nel_include_subdomains": data['nel_include_subdomains']
    })
    del data
    gc.collect()

    is_data = is_data.groupby(['date', 'url_domain'], as_index=False, observed=False).first()
    is_data_length = len(is_data)
    is_data = is_data.groupby(['date', 'nel_include_subdomains'], as_index=False, observed=False).agg(
        domain_count=("url_domain", "count"))
    is_data['domain_percent'] = is_data['domain_count'] / is_data_length * 100

    #
    # max_age
    #
    data = pd.read_parquet(input_file, columns=['url_domain', 'nel_max_age'])
    ma_data = DataFrame({
        "date": [f"{year}-{month}"] * len(data),
        "url_domain": data['url_domain'],
        "nel_max_age": data['nel_max_age']
    })
    del data
    gc.collect()

    ma_data = ma_data.groupby(['date', 'url_domain'], as_index=False, observed=False).first()
    ma_data_length = len(ma_data)
    ma_data = ma_data.groupby(['date', 'nel_max_age'], as_index=True, observed=False).agg(
        domain_count=("url_domain", "count"))
    ma_data.reset_index(inplace=True)  # Used as_index=True here because of unexpected an index length problem

    ma_data['nel_max_age'] = ma_data['nel_max_age'].astype("UInt64")
    ma_data.sort_values(by='nel_max_age', ascending=True, inplace=True)

    ma_data['domain_percent'] = ma_data['domain_count'] / ma_data_length * 100

    result = {
        "failure_fraction": ff_data.reset_index(drop=True),
        "success_fraction": sf_data.reset_index(drop=True),
        "include_subdomains": is_data.reset_index(drop=True),
        "max_age": ma_data.reset_index(drop=True)
    }

    return result


def produce_output_nel_config(aggregated_metric: Dict[str, DataFrame]):
    [metric.to_html(f"out/nel_config-{metric_name}.html") for (metric_name, metric) in aggregated_metric.items()]


def update_monitored_resource_type(input_file: Path, year: str, month: str, used_psl: StringIO) -> DataFrame:
    """TODO generates GBs of html data - fix pls"""
    data = pd.read_parquet(input_file, columns=['url_domain', 'type'])

    data['date'] = f"{year}-{month}"
    data['tmp'] = 1  # Prepare temporary column for counting instances of a monitored type per url_domain
    result = data.groupby(['date', 'url_domain', 'type'], as_index=True, observed=False).agg(count=('tmp', 'count'))
    result.reset_index(inplace=True)  # Used as_index=True here because of unexpected an index length problem

    return result


def produce_output_nel_monitored_resource_type(aggregated_metric: DataFrame):
    aggregated_metric.to_html("out/nel_monitored_resource_type.html")
