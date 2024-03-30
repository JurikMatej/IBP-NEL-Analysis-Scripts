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
from src import metric_utils


RESOURCE_BATCH_SIZE = 50_000_000

"""
METRIC LEGEND: (see docs/data-contract)
    cX = custom metric number X
    bX = base metric number X
    cX (bY) = custom metric number X - fulfills Y from base metric b
"""

def update_nel_deployment(input_file: Path, year: str, month: str) -> DataFrame:
    """PREPARES DATA FOR: c2 (b1)"""

    # Read only the first row of the month data file (necessary data is already precomputed)
    parquet = pq.ParquetFile(input_file)
    first_row = next(parquet.iter_batches(batch_size=1,
                                          columns=[
                                              'total_crawled_resources',
                                              'total_crawled_domains',
                                              'total_crawled_resources_with_nel',
                                              'total_crawled_domains_with_nel',
                                              'total_crawled_resources_with_correct_nel',
                                              'total_crawled_domains_with_correct_nel']))
    data = pa.Table.from_batches([first_row]).to_pandas()

    if len(data) == 0:
        # TODO no data = no record of total crawled domains
        return DataFrame()

    total_crawled_resources = data['total_crawled_resources'][0]
    total_crawled_domains = data['total_crawled_domains'][0]
    total_crawled_resources_with_nel = data['total_crawled_resources_with_nel'][0]
    total_crawled_domains_with_nel = data['total_crawled_domains_with_nel'][0]
    total_crawled_resources_with_correct_nel = data['total_crawled_resources_with_correct_nel'][0]
    total_crawled_domains_with_correct_nel = data['total_crawled_domains_with_correct_nel'][0]

    # TODO do not forget to add this col in the visualize step
    # nel_percentage = np.uint32(total_nel_domains) / np.uint32(total_domains) * 100

    result = DataFrame({
        "date": [f"{year}-{month}"],
        "total_crawled_resources": [total_crawled_resources],
        "total_crawled_domains": [total_crawled_domains],
        "total_crawled_resources_with_nel": [total_crawled_resources_with_nel],
        "total_crawled_domains_with_nel": [total_crawled_domains_with_nel],
        "total_crawled_resources_with_correct_nel": [total_crawled_resources_with_correct_nel],
        "total_crawled_domains_with_correct_nel": [total_crawled_domains_with_correct_nel],
    }).reset_index(drop=True)

    del data
    gc.collect()

    return result


def produce_output_yearly_nel_deployment(aggregated_metric: DataFrame):
    aggregated_metric['domains'] = aggregated_metric['domains'].astype(int)
    aggregated_metric['nel'] = aggregated_metric['nel'].astype(int)
    aggregated_metric.to_html("out/nel_deployment.html")


def update_nel_collector_provider_usage(input_file: Path, aggregated_providers: Series, year: str, month: str,
                                        used_psl: StringIO) -> DataFrame:
    """PREPARES DATA FOR: c1, c2 (b2 & b3), c4, c5"""

    # TODO use the used_psl to parse collector names instead of using the rt_collectors_registrable col
    data = pd.read_parquet(input_file,
                           columns=['url_domain', 'rt_collectors_registrable'])

    collectors_per_url_domain = data.groupby(['url_domain'], observed=True).first()

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

    ff_data = ff_data.groupby(['date', 'url_domain'], as_index=False, observed=True).first()
    ff_data_length = len(ff_data)
    ff_data = ff_data.groupby(['date', 'nel_failure_fraction'], as_index=False, observed=True).agg(
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

    sf_data = sf_data.groupby(['date', 'url_domain'], as_index=False, observed=True).first()
    sf_data_length = len(sf_data)
    sf_data = sf_data.groupby(['date', 'nel_success_fraction'], as_index=False, observed=True).agg(
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

    is_data = is_data.groupby(['date', 'url_domain'], as_index=False, observed=True).first()
    is_data_length = len(is_data)
    is_data = is_data.groupby(['date', 'nel_include_subdomains'], as_index=False, observed=True).agg(
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

    ma_data = ma_data.groupby(['date', 'url_domain'], as_index=False, observed=True).first()
    ma_data_length = len(ma_data)
    ma_data = ma_data.groupby(['date', 'nel_max_age'], as_index=True, observed=True).agg(
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


def nel_resource_config_variability(input_file: Path, year: str, month: str, used_psl: StringIO):
    """PREPARES DATA FOR: c6"""
    parquet = pq.ParquetFile(input_file)
    data_batch_generator = parquet.iter_batches(batch_size=RESOURCE_BATCH_SIZE,
                                                columns=[
                                                    'url_domain',
                                                    'nel_include_subdomains',
                                                    'nel_failure_fraction',
                                                    'nel_success_fraction',
                                                    'nel_max_age'
                                                ])
    result = None

    for batch in data_batch_generator:
        data = batch.to_pandas()

        data['resources_with_this_config'] = 1
        batch_result = data.groupby(
            ['url_domain', 'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'],
            observed=True
        ).agg({'resources_with_this_config': 'count'})

        batch_result.reset_index(inplace=True)
        batch_result = batch_result[(batch_result.duplicated(subset=['url_domain'], keep=False))]

        if result is None:
            # Store results the first time (or at least the schema if batch_result is empty)
            result = batch_result
        else:
            if not batch_result.empty:
                result = pd.concat([result, batch_result])

    # Perform one more group by to handle in-between-the-batches duplicate results
    # If the same NEL config is found for 2 domains, sum their resources_with_this_config numbers
    result = result.groupby(
            ['url_domain', 'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'],
            observed=True, as_index=False
        ).agg({'resources_with_this_config': 'sum'})

    result['date'] = f"{year}-{month}"
    result.set_index('date', inplace=True)
    result.reset_index(inplace=True)

    # TODO save the result instead
    # return result


def update_monitored_resource_type(input_file: Path, year: str, month: str, used_psl: StringIO) -> DataFrame:
    """
    PREPARES DATA FOR: c8

    TODO generates GBs of html data - fix pls
    """
    data = pd.read_parquet(input_file, columns=['url_domain', 'type'])

    data['date'] = f"{year}-{month}"
    data['tmp'] = 1  # Prepare temporary column for counting instances of a monitored type per url_domain
    result = data.groupby(['date', 'url_domain', 'type'], as_index=False, observed=True).agg(count=('tmp', 'count'))

    # TODO save per month here; the resource based metrics generate too large results

    del data
    gc.collect()

    return result


def produce_output_nel_monitored_resource_type(aggregated_metric: DataFrame):
    aggregated_metric.to_html("out/nel_monitored_resource_type.html")


def nel_popular_deployment(input_file: Path, year: str, month: str):
    """PREPARES DATA FOR: c9a"""

    # Load data & group by url_domain right away
    # because the other columns have the same value for each url_domain resource
    nel_data = pd.read_parquet(input_file, columns=[
        'url_domain',
        'url_domain_hosted_resources',
        'url_domain_hosted_resources_with_nel',
        'url_domain_monitored_resources_ratio',
    ]).groupby(['url_domain'], as_index=False).first().reset_index(drop=True)

    tranco_list = metric_utils.load_tranco_list_for_current_month(year, month)
    if tranco_list is None:
        return

    result = nel_data[nel_data['url_domain'].isin(tranco_list['popular_domain_name'])]

    result = result.copy()
    result['date'] = f"{year}-{month}"
    result.set_index('date', inplace=True)
    result.reset_index(inplace=True)

    # TODO save per month
    return result


def nel_popular_collector_provider_usage(input_file: Path, aggregated_providers: Series, year: str, month: str,
                                         used_psl: StringIO):
    """
    PREPARES DATA FOR: c9b, c9d

    TODO very probably straight up duplicate
            - this can be computed from the base collector providers metric during the visualize step
    """

    data = pd.read_parquet(input_file, columns=['url_domain', 'rt_collectors_registrable'])

    tranco_list = metric_utils.load_tranco_list_for_current_month(year, month)
    if tranco_list is None:
        return

    collectors_per_url_domain = data.groupby(['url_domain'], as_index=False, observed=True).first()

    del data
    gc.collect()

    collectors_per_popular_url_domain = collectors_per_url_domain.copy()[
        collectors_per_url_domain['url_domain'].isin(tranco_list['popular_domain_name'])
    ]

    del collectors_per_url_domain
    gc.collect()

    # Not extracting to a common function with nel_collector_provider_usage()
    # to avoid passing large dataframes to functions by value
    total_url_domains = len(collectors_per_popular_url_domain)

    all_providers_so_far = Series(np.append(aggregated_providers.values,
                                            collectors_per_popular_url_domain['rt_collectors_registrable'].explode().unique()
                                            )).dropna().unique()
    result = DataFrame({
        "date": [f"{year}-{month}"] * len(all_providers_so_far),
        "providers": all_providers_so_far
    })

    collectors_per_popular_url_domain['primary_collectors'] = collectors_per_popular_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[0] if len(collectors) > 0 else None
    )
    collectors_per_popular_url_domain['secondary_collectors'] = collectors_per_popular_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[1] if len(collectors) > 1 else None
    )
    collectors_per_popular_url_domain['fallback_collectors'] = collectors_per_popular_url_domain['rt_collectors_registrable'].map(
        lambda collectors: collectors[2:] if len(collectors) > 2 else None
    )

    collectors_per_popular_url_domain = collectors_per_popular_url_domain.drop(columns=['rt_collectors_registrable'])

    collectors_per_popular_url_domain.dropna(inplace=True, subset=['primary_collectors'])
    collectors_per_popular_url_domain.reset_index(inplace=True)

    primary_collector_usage = (collectors_per_popular_url_domain
                               .groupby(['primary_collectors'])
                               .agg(as_primary=('url_domain', 'count')))
    primary_collector_usage.reset_index(inplace=True)

    secondary_collector_usage = (collectors_per_popular_url_domain
                                 .groupby(['secondary_collectors'])
                                 .agg(as_secondary=('url_domain', 'count')))
    secondary_collector_usage.reset_index(inplace=True)

    fallback_collectors_per_popular_url_domain = collectors_per_popular_url_domain[['url_domain', 'fallback_collectors']]
    fallback_collector_usage = (fallback_collectors_per_popular_url_domain
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

    result.reset_index(inplace=True, drop=True)

    # TODO save results

    return result['providers']


def nel_popular_resource_config(input_file: Path, year: str, month: str):
    """
    PREPARES DATA FOR: c9c
    
    TODO use during visualize - this can be computed from the already existing resource config metric data
    """
    data_columns_config_per_url_domain = [
        'url_domain', 'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'
    ]
    data_columns_config_settings = [
        'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'
    ]

    nel_data = pd.read_parquet(input_file, columns=data_columns_config_per_url_domain)
    tranco_list = metric_utils.load_tranco_list_for_current_month(year, month)

    data = nel_data[nel_data['url_domain'].isin(tranco_list['popular_domain_name'])].copy()
    data['resources_with_this_config'] = 1

    # Count config variation occurrences in the resources hosted on popular domains (group by unique url_domain config)
    result = data.groupby(data_columns_config_per_url_domain, observed=True).agg({
        'resources_with_this_config': 'count'
    })

    result.reset_index(inplace=True)

    # Among the config variations for all popular domains found, count the config variation occurrences
    # (group by unique config - this time count the domains using each unique config variation)
    result = result.groupby(data_columns_config_settings, observed=True, as_index=False).agg(
        domains_using_this_config=('url_domain', 'count'))

    result.sort_values(by='domains_using_this_config', ascending=False, inplace=True)

    # Add date as the leftmost column
    result['date'] = f"{year}-{month}"
    result.set_index('date', inplace=True)
    result.reset_index(inplace=True)

    # TODO save the result instead
    return result
