#!/usr/bin/env python3
"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

TODO
    1. Use the preprocessed data obtained from the output of preprocess_stored.py
    2. Create a set of functions computing the metrics agreed upon. These functions will create:
        a. Statements - describe a fact about NEL deployment (% of NEL deployments)
        b. Visualizations - additionally, if applicable, generate a graph displaying the metrics
                            in an easily understandable way
        c. Latex src text - if possible, also implement a latex file output capability that can than be leveraged
                            to save time when re-running the analysis later (save the statement and the visualization
                            as a latex file)
    A.
        The goal is to have the analysis located in one place (one runnable script) so that it is not so cumbersome to
        reproduce results or generate more results at a later time
        (in comparison to using the strategy to implement multiple jupyter notebooks)
    B.
        Keep in mind that this script should be doing as less as possible.
        Ideally, only load the data once (but it's a LOT of data)
        Ideally, compute all the metrics with it (implement the common structure well in the postprocess_stored.py)
        Ideally, output should be easily usable for the thesis - copy and paste .tex, or upload image to be used
            as a figure (this takes quite a lot of time and is not required by the analysis assignment)
"""

import logging
import sys
import pathlib
from typing import List

import pandas as pd

import src.nel_analyis as nel_analysis

# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


###############################
# CONFIGURE THESE BEFORE USE: #
###############################

# Download directory structure
NEL_DATA_DIR_PATH = "httparchive_data_raw"


def main():
    nel_data_dir = pathlib.Path(NEL_DATA_DIR_PATH)
    nel_data_dir.mkdir(exist_ok=True, parents=True)

    input_files = list(nel_data_dir.glob("nel_data_*.parquet"))

    metrics_aggregates = {
        "yearly_nel_deployment": pd.DataFrame({
            "date": [],
            "domains": [],
            "nel": [],
            "nel_percentage": []
        }),
        "nel_collector_providers": pd.DataFrame()
    }

    logger.info(f"Analyzing metrics for all files in ---{NEL_DATA_DIR_PATH}---")
    print()

    if len(input_files) < 1:
        logger.error("No input files found")
    else:
        run_analysis(input_files, metrics_aggregates)


def run_analysis(input_files: List[pathlib.Path], metrics_aggregates: dict[str, pd.DataFrame]):
    for input_file in input_files:
        logger.info(f"---{input_file.name.upper()}---")

        month, year = input_file.stem.split("_")[::-1][:2]  # Reverse and take last 2 values
        month_data = pd.read_parquet(input_file)

        yearly_nel_deployment_next = nel_analysis.yearly_nel_deployment_next(month_data, year, month)
        metrics_aggregates['yearly_nel_deployment'] = pd.concat([metrics_aggregates['yearly_nel_deployment'], yearly_nel_deployment_next])

        nel_analysis.update_nel_collector_providers(month_data, metrics_aggregates["nel_collector_providers"], year, month)

    nel_analysis.produce_output_yearly_nel_deployment(metrics_aggregates['yearly_nel_deployment'])
    nel_analysis.produce_output_nel_collector_providers(metrics_aggregates['nel_collector_providers'])


if __name__ == "__main__":
    main()
