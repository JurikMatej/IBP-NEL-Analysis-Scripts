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

import numpy as np

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
        run_analysis(input_files, psl_files)


def run_analysis(input_files: List[pathlib.Path], psl_files: List[pathlib.Path]):
    # Initialize data to be aggregated throughout the months
    collector_providers_so_far = np.empty(0, dtype=str)

    # Monthly analysis loop
    for input_file in input_files:
        logger.info(f"---{input_file.name.upper()}---")

        # Convention: nel_data_YYYY_MM.parquet
        month, year = input_file.stem.split("_")[::-1][:2]  # Reverse and take last 2 values
        date = f"{year}-{month}"

        psl = psl_utils.get_psl_for_specific_date(year, month, PSL_DIR_PATH, psl_files)

        # The used PSL library needs the PSL as file-like type to read from
        # So to avoid saving temporary files to disk, StringIO() is used
        with io.StringIO() as psl_IO:
            psl_IO.write(psl)

            # Deployment data
            nel_analysis.nel_deployment(input_file, date)

            # Domain deployment stats data
            nel_analysis.nel_domain_resource_monitoring_stats(input_file, date)

            # Collector data (aggregate unique collector providers throughout the analyzed months)
            collector_providers_so_far = nel_analysis.nel_collector_provider_usage(input_file,
                                                                                   collector_providers_so_far,
                                                                                   date,
                                                                                   reset_psl_file(psl_IO))

            # Domain configuration data
            nel_analysis.nel_config(input_file, date)

            # Resource configuration data
            nel_analysis.nel_resource_config_variability(input_file, date)

            # Resource type data
            nel_analysis.nel_monitored_resource_types(input_file, date)

            gc.collect()

        print()

    logger.info("Done. Exiting...")


def reset_psl_file(psl_file: io.StringIO) -> io.StringIO:
    psl_file.seek(0)
    return psl_file


if __name__ == "__main__":
    main()
