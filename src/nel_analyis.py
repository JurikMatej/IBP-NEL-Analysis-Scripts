import gc
import os
from io import StringIO

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame, Series
from pathlib import Path

from src import psl_utils


RESOURCE_BATCH_SIZE = 50_000_000


"""
METRIC LEGEND: (see docs/data-contract)
    cX = custom metric number X
    bX = base metric number X
    cX (bY) = custom metric number X - fulfills Y from base metric b
"""


def nel_deployment(input_file: Path, date: str, output_dir: str):
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
        return

    total_crawled_resources = data['total_crawled_resources'][0]
    total_crawled_domains = data['total_crawled_domains'][0]
    total_crawled_resources_with_nel = data['total_crawled_resources_with_nel'][0]
    total_crawled_domains_with_nel = data['total_crawled_domains_with_nel'][0]
    total_crawled_resources_with_correct_nel = data['total_crawled_resources_with_correct_nel'][0]
    total_crawled_domains_with_correct_nel = data['total_crawled_domains_with_correct_nel'][0]

    result = DataFrame({
        "date": [date],
        "total_crawled_resources": [total_crawled_resources],
        "total_crawled_domains": [total_crawled_domains],
        "total_crawled_resources_with_nel": [total_crawled_resources_with_nel],
        "total_crawled_domains_with_nel": [total_crawled_domains_with_nel],
        "total_crawled_resources_with_correct_nel": [total_crawled_resources_with_correct_nel],
        "total_crawled_domains_with_correct_nel": [total_crawled_domains_with_correct_nel],
    }).reset_index(drop=True)

    del data
    gc.collect()

    # Save the result
    output_dir = Path(f"{output_dir}/nel_deployment")
    output_dir.mkdir(parents=True, exist_ok=True)

    result.to_parquet(os.path.join(output_dir.absolute(), f"{date}.parquet"))


def nel_domain_resource_monitoring_stats(input_file: Path, date: str, output_dir: str):
    """PREPARES DATA FOR: c9a"""

    # Load data & group by url_domain right away
    # because the other columns have the same value for each url_domain resource
    result = pd.read_parquet(input_file, columns=[
        'url_domain',
        'url_domain_hosted_resources',
        'url_domain_hosted_resources_with_nel',
        'url_domain_monitored_resources_ratio',
    ]).groupby(['url_domain'], observed=True, as_index=False).first().reset_index(drop=True)

    result = result.copy()
    result['date'] = date
    result.set_index('date', inplace=True)
    result.reset_index(inplace=True)

    # Save the result
    output_dir = Path(f"{output_dir}/nel_domain_resource_monitoring_stats")
    output_dir.mkdir(parents=True, exist_ok=True)

    result.to_parquet(os.path.join(output_dir.absolute(), f"{date}.parquet"))


def nel_collector_provider_usage(input_file: Path, aggregated_providers: np.ndarray, date: str,
                                 used_psl: StringIO | None, output_dir: str) -> np.ndarray:
    """PREPARES DATA FOR: c1, c2 (b2 & b3), c4, c5"""

    if used_psl is None:
        used_columns = ['url_domain', 'rt_collectors_registrable']
    else:
        used_columns = ['url_domain', 'rt_collectors']

    data = pd.read_parquet(input_file, columns=used_columns)
    collectors_per_url_domain = data.groupby(['url_domain'], observed=True).first()

    del data
    gc.collect()

    total_url_domains = len(collectors_per_url_domain)

    collectors_column = used_columns[1]
    # In case the argument used_psl was provided, map each collector domain name into it's registrable domain name
    if used_psl is not None:
        unique_fullname_domains = collectors_per_url_domain[collectors_column].explode().dropna().unique()
        unique_registrable_domains = np.asarray(
            [psl_utils.get_sld_from_custom_psl(domain, used_psl) for domain in unique_fullname_domains]
        )
        registrable_domain_map = dict(zip(unique_fullname_domains, unique_registrable_domains))

        collectors_per_url_domain[collectors_column] = collectors_per_url_domain[collectors_column].map(
            lambda collectors:
                [registrable_domain_map[domain_name] for domain_name in collectors]
        )

    all_providers_so_far = Series(
        np.append(
            aggregated_providers,
            collectors_per_url_domain[collectors_column].explode().unique()
        )
    ).dropna().unique()

    result = DataFrame({
        "date": [date] * len(all_providers_so_far),
        "providers": all_providers_so_far
    })

    collectors_per_url_domain['primary_collectors'] = collectors_per_url_domain[collectors_column].map(
        lambda collectors: collectors[0] if len(collectors) > 0 else None
    )
    collectors_per_url_domain['secondary_collectors'] = collectors_per_url_domain[collectors_column].map(
        lambda collectors: collectors[1] if len(collectors) > 1 else None
    )
    collectors_per_url_domain['fallback_collectors'] = collectors_per_url_domain[collectors_column].map(
        lambda collectors: collectors[2:] if len(collectors) > 2 else None
    )

    collectors_per_url_domain = collectors_per_url_domain.drop(columns=[collectors_column])

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

    result.sort_values(by=['as_primary'], ascending=False, inplace=True)
    result.reset_index(inplace=True, drop=True)

    # Save the result
    output_dir = Path(f"{output_dir}/nel_collector_provider_usage")
    output_dir.mkdir(parents=True, exist_ok=True)

    result.to_parquet(os.path.join(output_dir.absolute(), f"{date}.parquet"))

    # Remember every analyzed collector provider from the beginning
    return all_providers_so_far


def nel_config(input_file: Path, date: str, output_dir: str):
    """
    PREPARES DATA FOR: c7

    IMPL NOTE: I deliberately avoid loading all the data and making copies of it here to trade computing time for RAM.
    """
    #
    # failure_fraction
    #
    data = pd.read_parquet(input_file, columns=['url_domain', 'nel_failure_fraction'])
    ff_data = DataFrame({
        "date": [date] * len(data),
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
        "date": [date] * len(data),
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
        "date": [date] * len(data),
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
        "date": [date] * len(data),
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

    # Gather the results
    result_failure_fraction = ff_data.reset_index(drop=True)
    result_success_fraction = sf_data.reset_index(drop=True)
    result_include_subdomains = is_data.reset_index(drop=True)
    result_max_age = ma_data.reset_index(drop=True)

    # Save the results
    output_dir = Path(f"{output_dir}/nel_config")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_dir_absolute = output_dir.absolute()

    failure_fraction_output_path = os.path.join(output_dir_absolute, f"failure_fraction_{date}.parquet")
    result_failure_fraction.to_parquet(failure_fraction_output_path)

    success_fraction_output_path = os.path.join(output_dir_absolute, f"success_fraction_{date}.parquet")
    result_success_fraction.to_parquet(success_fraction_output_path)

    include_subdomains_output_path = os.path.join(output_dir_absolute, f"include_subdomains_{date}.parquet")
    result_include_subdomains.to_parquet(include_subdomains_output_path)

    max_age_output_path = os.path.join(output_dir_absolute, f"max_age_{date}.parquet")
    result_max_age.to_parquet(max_age_output_path)


def nel_resource_config_variability(input_file: Path, date: str, output_dir: str):
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

        # TODO observe duplication (the line below) or config usage (tranco variation of this method)
        #      only during the visualize phase
        # batch_result = batch_result[(batch_result.duplicated(subset=['url_domain'], keep=False))]

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

    result['date'] = date
    result.set_index('date', inplace=True)
    result.reset_index(inplace=True)

    # Cast to category where convenient
    result['date'] = result['date'].astype("category")
    result['nel_include_subdomains'] = result['nel_include_subdomains'].astype("category")
    result['nel_failure_fraction'] = result['nel_failure_fraction'].astype("category")
    result['nel_success_fraction'] = result['nel_success_fraction'].astype("category")
    result['nel_max_age'] = result['nel_max_age'].astype("category")

    # Save the result
    output_dir = Path(f"{output_dir}/nel_resource_config_variability")
    output_dir.mkdir(parents=True, exist_ok=True)

    result.to_parquet(os.path.join(output_dir.absolute(), f"{date}.parquet"))


def nel_monitored_resource_types(input_file: Path, date: str, output_dir: str):
    """PREPARES DATA FOR: c8"""
    data = pd.read_parquet(input_file, columns=['url_domain', 'type'])

    data['date'] = date
    data['tmp'] = 1  # Prepare temporary column for counting instances of a monitored type per url_domain
    result = data.groupby(['date', 'url_domain', 'type'], as_index=False, observed=True).agg(count=('tmp', 'count'))

    del data
    gc.collect()

    # Cast to convenient types
    result['date'] = result['date'].astype("category")
    result['url_domain'] = result['url_domain'].astype("category")
    result['type'] = result['type'].astype("category")
    result['count'] = result['count'].astype("UInt32")

    # Save the result
    output_dir = Path(f"{output_dir}/nel_monitored_resource_types")
    output_dir.mkdir(parents=True, exist_ok=True)

    result.to_parquet(os.path.join(output_dir.absolute(), f"{date}.parquet"))


# TODO use during the visualize phase - this can be computed from the already existing resource config metric data
# def nel_popular_resource_config(input_file: Path, year: str, month: str):
#     """PREPARES DATA FOR: c9c"""
#     data_columns_config_per_url_domain = [
#         'url_domain', 'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'
#     ]
#     data_columns_config_settings = [
#         'nel_include_subdomains', 'nel_failure_fraction', 'nel_success_fraction', 'nel_max_age'
#     ]
#
#     nel_data = pd.read_parquet(input_file, columns=data_columns_config_per_url_domain)
#     tranco_list = metric_utils.load_tranco_list_for_current_month(year, month)
#
#     data = nel_data[nel_data['url_domain'].isin(tranco_list['popular_domain_name'])].copy()
#     data['resources_with_this_config'] = 1
#
#     # Count config variation occurrences in the resources hosted on popular domains (group by unique url_domain config)
#     result = data.groupby(data_columns_config_per_url_domain, observed=True).agg({
#         'resources_with_this_config': 'count'
#     })
#
#     result.reset_index(inplace=True)
#
#     # Among the config variations for all popular domains found, count the config variation occurrences
#     # (group by unique config - this time count the domains using each unique config variation)
#     result = result.groupby(data_columns_config_settings, observed=True, as_index=False).agg(
#         domains_using_this_config=('url_domain', 'count'))
#
#     result.sort_values(by='domains_using_this_config', ascending=False, inplace=True)
#
#     # Add date as the leftmost column
#     result['date'] = f"{year}-{month}"
#     result.set_index('date', inplace=True)
#     result.reset_index(inplace=True)
#
#     # DO NOTHING - this function is to be removed when rewritten into the visualize phase script
