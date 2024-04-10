#!/usr/bin/env python3

"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

description:    Descriptive..

purpose:        Purposeful...
"""


import logging
import sys
from io import StringIO
from pathlib import Path

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

# Crawl directory structure
CRAWL_DATA_DIR_PATH = "data/crawled_raw"
PSL_DIR_PATH = "resources/public_suffix_lists"

ANALYSIS_OUTPUT_DIR = "data/crawled_metrics/"


def main():
    crawl_data_dir = Path(CRAWL_DATA_DIR_PATH)
    crawl_data_dir.mkdir(exist_ok=True, parents=True)
    input_files = list(crawl_data_dir.glob("merged_*.parquet"))

    if len(input_files) < 1:
        logger.error("No crawled data available. Run the crawler first")
        return

    # Use the latest crawled data file
    input_file = input_files[-1]

    psl_dir = Path(PSL_DIR_PATH)
    psl_dir.mkdir(exist_ok=True, parents=True)

    psl_to_use = Path(f"{PSL_DIR_PATH}/psl_current.dat")

    if not psl_to_use.exists() or not psl_to_use.is_file():
        logger.error(f"Please provide the current Public Suffix List as ({PSL_DIR_PATH}/psl_current.dat)")
        return

    logger.info(f"Analyzing metrics for CRAWLED data file ---{input_file.name}---")

    run_analysis(input_file, psl_to_use)


def run_analysis(input_file: Path, psl_file: Path):
    # Convention: merged_YYYYMMDD-HHmmSS.parquet
    date_string = input_file.stem.split("_")[1].split('-')[0]
    year = date_string[:4]
    month = date_string[4:6]

    date = f"{year}-{month}"

    # No option to use pre-computed registrable domain data - use a PSL file, always
    psl = psl_utils.get_psl_by_path(psl_file)

    # The used PSL library needs the PSL as file-like type to read from
    # So to avoid saving temporary files to disk, StringIO() is used
    with StringIO() as psl_io:
        psl_io.write(psl)
        psl_io.seek(0)

        # Deployment data
        nel_analysis.nel_deployment(input_file, date, ANALYSIS_OUTPUT_DIR)

        # Domain deployment stats data
        nel_analysis.nel_domain_resource_monitoring_stats(input_file, date, ANALYSIS_OUTPUT_DIR)

        # Collector data
        # (no months to aggregate collector providers over, just passing emtpy array as the aggregated value)
        collector_providers_so_far = np.empty(0, dtype=str)
        nel_analysis.nel_collector_provider_usage(input_file, collector_providers_so_far, date, psl_io,
                                                  ANALYSIS_OUTPUT_DIR)

        # Domain configuration data
        nel_analysis.nel_config(input_file, date, ANALYSIS_OUTPUT_DIR)

        # Resource configuration data
        nel_analysis.nel_resource_config_variability(input_file, date, ANALYSIS_OUTPUT_DIR)

        # Resource type data
        nel_analysis.nel_monitored_resource_types(input_file, date, ANALYSIS_OUTPUT_DIR)

    logger.info("Done. Exiting...")


if __name__ == "__main__":
    main()

