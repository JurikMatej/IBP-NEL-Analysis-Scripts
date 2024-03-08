#!/usr/bin/env python3
"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

description:

purpose:


TODO
    5. Organize data to an optimal structure that can be used in all metric-computing analysis scripts
"""


import sys
from typing import List

import pandas as pd
import json
import pathlib
import logging


###############################
# CONFIGURE THESE BEFORE USE: #
###############################
DOWNLOAD_OUTPUT_DIR_PATH = "httparchive_data_raw"
DOWNLOAD_CONFIG_PATH = "download_config.json"

POSTPROCESS_OUTPUT_DIR_PATH = "httparchive_data_processed"

# PUBLIC_SUFFIX_LISTS_DIR = "public_suffix_lists"


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s:\t%(message)s')
logger = logging.getLogger(__name__)


def run_postprocess(input_files_desktop: List[str], input_files_mobile: List[str]) -> pd.DataFrame:
    # TODO total_* fields are probably unmerge-able
    desktop_df = load_and_merge_month_data(input_files_desktop)
    mobile_df = load_and_merge_month_data(input_files_mobile)

    # TODO merge merges to col_x and col_y, resulting in 2x the columns
    merged_df = merge_desktop_with_mobile(desktop_df, mobile_df)

    if not merged_df.empty:
        add_public_suffix_to_data(merged_df)
        # ... additional magic to prepare data for analysis

    return merged_df


def load_and_merge_month_data(source_file_names: List[str]) -> pd.DataFrame:
    if len(source_file_names) < 1:
        return pd.DataFrame()

    result = pd.DataFrame()
    for source_file_name in source_file_names:
        logger.debug(f"Loading {DOWNLOAD_OUTPUT_DIR_PATH}/{source_file_name}.parquet")
        tmp_df = pd.read_parquet(f"{DOWNLOAD_OUTPUT_DIR_PATH}/{source_file_name}.parquet")

        if result.empty:
            result = tmp_df
        else:
            # TODO consult with supervisor - LEFT JOIN causes 01.MM.20YY NEL data to override eg.: 15.MM.20YY
            result = pd.concat([result, tmp_df]).drop_duplicates(['url']).reset_index()

    return result


def merge_desktop_with_mobile(desktop_df: pd.DataFrame, mobile_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug("Merging desktop with mobile data")
    logger.debug(f"Desktop entries: {desktop_df['url'].size}")
    logger.debug(f"Mobile entries: {mobile_df['url'].size}")

    try:
        # Merge using Desktop as the default on conflict
        # return desktop_df.merge(mobile_df, how="left", on="url")
        return pd.concat([desktop_df, mobile_df]).drop_duplicates(['url']).reset_index()
    except IndexError:
        # Upon one of the DFs being empty
        return desktop_df if not desktop_df.empty else mobile_df


def add_public_suffix_to_data(df: pd.DataFrame) -> pd.DataFrame:
    return df


def main():
    with open(DOWNLOAD_CONFIG_PATH, 'r') as config_file:
        download_conf = json.loads(config_file.read())

        for item in download_conf:
            # Gather both desktop and mobile source data file names
            input_files_desktop = item.get("input_desktop", [])
            input_files_mobile = item.get("input_mobile", [])

            # Create output directory if not exists
            output_dir = pathlib.Path(POSTPROCESS_OUTPUT_DIR_PATH)
            if not output_dir.is_dir():
                output_dir.mkdir(parents=True)

            output_file_name = item.get("processed_output")
            output_file = pathlib.Path(f"{POSTPROCESS_OUTPUT_DIR_PATH}/{output_file_name}.parquet")

            # Check whether the post processed file already exists
            if output_file.is_file():
                logger.info(f"Table {output_file_name} already among processed files")
                continue

            df = run_postprocess(input_files_desktop, input_files_mobile)
            df.to_parquet(output_file)


if __name__ == "__main__":
    main()
