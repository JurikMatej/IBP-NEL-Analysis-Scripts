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
from typing import List

import pandas as pd

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
    "monthly_nel_deployment": pd.DataFrame({
        "date": [],
        "domains": [],
        "nel": [],
        "nel_percentage": []
    }),
    "nel_collector_provider_usage": pd.DataFrame({
        "date": [],
        "providers": [],
        "as_primary": [],
        "share_as_primary": [],
        "as_secondary": [],
        "share_as_secondary": [],
        "among_fallback": [],
    })
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
                 metric_aggregates: dict[str, pd.DataFrame]):
    for input_file in input_files:
        logger.info(f"---{input_file.name.upper()}---")

        # Convention: nel_data_YYYY_MM.parquet
        month, year = input_file.stem.split("_")[::-1][:2]  # Reverse and take last 2 values

        psl = psl_utils.get_psl_for_specific_date(year, month, PSL_DIR_PATH, psl_files)

        # The used PSL library needs the PSL as file-like type to read from
        # So to avoid saving temporary files to disk, StringIO() is used
        with (io.StringIO() as psl_IO):
            psl_IO.write(psl)

            # Deployment data
            monthly_nel_deployment_next = nel_analysis.update_monthly_nel_deployment(
                input_file,
                year,
                month)
            metric_aggregates['monthly_nel_deployment'] = pd.concat(
                [metric_aggregates['monthly_nel_deployment'], monthly_nel_deployment_next])

            # Collector data
            nel_collector_usage_next = nel_analysis.update_nel_collector_provider_usage(
                input_file,
                metric_aggregates["nel_collector_provider_usage"]['providers'],
                year,
                month,
                reset_psl_file(psl_IO)
            )
            metric_aggregates["nel_collector_provider_usage"] = pd.concat(
                [metric_aggregates["nel_collector_provider_usage"], nel_collector_usage_next])

            gc.collect()

        print()

    # Deployment data output
    nel_analysis.produce_output_yearly_nel_deployment(metric_aggregates['monthly_nel_deployment'])

    # Collector data output
    nel_analysis.produce_output_nel_collector_provider_usage(metric_aggregates['nel_collector_provider_usage'])

    logger.info("Done. Exiting...")


def reset_psl_file(psl_file: io.StringIO) -> io.StringIO:
    psl_file.seek(0)
    return psl_file


if __name__ == "__main__":
    main()
