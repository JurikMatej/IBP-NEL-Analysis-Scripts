import gc
from io import StringIO

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame, Series
from pathlib import Path

from src import psl_utils

# METRIC LEGEND: (see docs/data-contract)
#   cX = custom metrics number X
#   bX = base metrics number X
#   cX (bY) = custom metrics number X - fulfills Y from base metrics b

# TODO PREPARE DATA FOR: c6, c7, c8


def update_monthly_nel_deployment(month_data_file: Path, year: str, month: str):
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
    aggregated_metric.to_html("out/monthly_nel_deployment.html")


def update_nel_collector_provider_usage(month_data_file: Path, aggregated_providers: Series, year: str, month: str,
                                        used_psl: StringIO):
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


def produce_output_nel_collector_provider_usage(aggregated_metric: pd.DataFrame):
    aggregated_metric.to_html("out/nel_collector_provider_usage.html")
