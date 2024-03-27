#!/usr/bin/env python3
"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

description:    Descriptive...

purpose:        Purposeful...
"""

import io
import logging
import sys
import pathlib
import gc
from typing import List, Dict

import pandas as pd
from pandas import DataFrame

import src.nel_analyis as nel_analysis
from src import psl_utils

# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)

###############################
# CONFIGURE THESE BEFORE USE: #
###############################

# Download directory structure
NEL_DATA_DIR_PATH = "httparchive_data_raw"
PSL_DIR_PATH = "public_suffix_lists"

# Pandas config (TODO use eventually only for visualization)
# pd.options.display.float_format = '{:.2f}'.format

# Metrics
METRIC_AGGREGATES = {
    "monthly_nel_deployment": DataFrame({
        "date": [],
        "domains": [],
        "nel": [],
        "nel_percentage": []
    }),
    "nel_collector_provider_usage": DataFrame({
        "date": [],
        "providers": [],
        "as_primary": [],
        "share_as_primary": [],
        "as_secondary": [],
        "share_as_secondary": [],
        "among_fallback": [],
    }),
    "nel_config": {
        "failure_fraction": DataFrame({
            "date": [],
            "nel_failure_fraction": [],
            "domain_count": [],
            "domain_percent": []
        }),
        "success_fraction": DataFrame({
            "date": [],
            "nel_success_fraction": [],
            "domain_count": [],
            "domain_percent": []
        }),
        "include_subdomains": DataFrame({
            "date": [],
            "nel_include_subdomains": [],
            "domain_count": [],
            "domain_percent": []
        }),
        "max_age": DataFrame({
            "date": [],
            "nel_max_age": [],
            "domain_count": [],
            "domain_percent": []
        })
    }
}


def main():
    nel_data_dir = pathlib.Path(NEL_DATA_DIR_PATH)
    nel_data_dir.mkdir(exist_ok=True, parents=True)
    input_files = list(nel_data_dir.glob("nel_data_*.parquet"))

    psl_dir = pathlib.Path(PSL_DIR_PATH)
    psl_dir.mkdir(exist_ok=True, parents=True)
    psl_files = list(psl_dir.glob("psl_*.dat"))
    if pathlib.Path(f"{PSL_DIR_PATH}/psl_current.dat") not in psl_files:
        logger.error(f"Please provide at least the current Public Suffix List ({PSL_DIR_PATH}/psl_current.dat).")
        return

    logger.info(f"Analyzing metrics for all files in ---{NEL_DATA_DIR_PATH}---")
    print()

    if len(input_files) < 1:
        logger.error("No input files found")
    else:
        run_analysis(input_files, psl_files, METRIC_AGGREGATES)


def run_analysis(input_files: List[pathlib.Path], psl_files: List[pathlib.Path],
                 metric_aggregates: Dict[str, DataFrame]):
    for input_file in input_files:
        logger.info(f"---{input_file.name.upper()}---")

        # Convention: nel_data_YYYY_MM.parquet
        month, year = input_file.stem.split("_")[::-1][:2]  # Reverse and take last 2 values

        psl = psl_utils.get_psl_for_specific_date(year, month, PSL_DIR_PATH, psl_files)

        # The used PSL library needs the PSL as file-like type to read from
        # So to avoid saving temporary files to disk, StringIO() is used
        with io.StringIO() as psl_IO:
            psl_IO.write(psl)

            # Deployment data
            monthly_nel_deployment_next = nel_analysis.update_monthly_nel_deployment(
                input_file,
                year,
                month)
            metric_aggregates['monthly_nel_deployment'] = concat_metric_aggregate(
                metric_aggregates['monthly_nel_deployment'],
                monthly_nel_deployment_next
            )

            # Collector data
            nel_collector_provider_usage = nel_analysis.update_nel_collector_provider_usage(
                input_file,
                metric_aggregates["nel_collector_provider_usage"]['providers'],
                year,
                month,
                reset_psl_file(psl_IO))
            metric_aggregates["nel_collector_provider_usage"] = concat_metric_aggregate(
                metric_aggregates["nel_collector_provider_usage"],
                nel_collector_provider_usage
            )

            nel_config_next = nel_analysis.update_nel_config(
                input_file,
                year,
                month,
                reset_psl_file(psl_IO)
            )
            metric_aggregates["nel_config"]['failure_fraction'] = \
                concat_metric_aggregate(metric_aggregates["nel_config"]['failure_fraction'],
                                        nel_config_next['failure_fraction'])

            metric_aggregates["nel_config"]['success_fraction'] = \
                concat_metric_aggregate(metric_aggregates["nel_config"]['success_fraction'],
                                        nel_config_next['success_fraction'])

            metric_aggregates["nel_config"]['include_subdomains'] = \
                concat_metric_aggregate(metric_aggregates["nel_config"]['include_subdomains'],
                                        nel_config_next['include_subdomains'])

            metric_aggregates["nel_config"]['max_age'] = \
                concat_metric_aggregate(metric_aggregates["nel_config"]['max_age'], nel_config_next['max_age'])

            gc.collect()

        print()

    # Deployment data output
    nel_analysis.produce_output_yearly_nel_deployment(metric_aggregates['monthly_nel_deployment'])

    # Collector data output
    nel_analysis.produce_output_nel_collector_provider_usage(metric_aggregates['nel_collector_provider_usage'])

    # Configuration data output
    nel_analysis.produce_output_nel_config(metric_aggregates['nel_config'])

    logger.info("Done. Exiting...")


def reset_psl_file(psl_file: io.StringIO) -> io.StringIO:
    psl_file.seek(0)
    return psl_file


def concat_metric_aggregate(aggregated: DataFrame, to_aggregate: DataFrame):
    if len(aggregated) == 0:
        return to_aggregate
    else:
        return pd.concat([aggregated, to_aggregate])


if __name__ == "__main__":
    main()
