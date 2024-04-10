#!/usr/bin/env python3

"""
Additional standalone script to merge crawled blobs data AD HOC
"""

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


###############################
# CONFIGURE THESE BEFORE USE: #
###############################
CRAWL_DATA_RAW_STORAGE_PATH = "data/crawled_raw/blobs"
CRAWL_DATA_STORAGE_PATH = "data/crawled_raw"


def merge_crawled_and_save(input_dir: str, output_dir: str):
    logger.info("Merging last crawled blob data")

    crawl_data_raw_path = Path(input_dir)
    crawl_data_raw_path.mkdir(parents=True, exist_ok=True)

    crawl_data_output_path = Path(output_dir)
    crawl_data_output_path.mkdir(parents=True, exist_ok=True)

    domain_data_files = list(crawl_data_raw_path.glob('*.parquet'))

    if len(domain_data_files) < 1:
        logger.warning("No crawled blob data files found. Aborting...")
        # Empty result
        return

    result_registry = CrawledDomainNelRegistry()
    start_time = time.monotonic()
    for domain_data_file in domain_data_files:
        domain_data_registry = CrawledDomainNelRegistry.read_raw(domain_data_file)
        result_registry.concat_content(domain_data_registry)

    logger.info(f"Merging finished in {time.monotonic() - start_time} seconds")

    current_datetime = datetime.now().strftime("%Y%m%d-%H%M%S")
    result_path = os.path.join(crawl_data_output_path.absolute(), f"merged_{current_datetime}.parquet")

    result_registry.count_totals()
    result_registry.filter_out_incorrect_nel()
    result_registry.save(result_path)
    logger.info(f"Merged data saved to {result_path}")


if __name__ == '__main__':
    merge_crawled_and_save(CRAWL_DATA_RAW_STORAGE_PATH, CRAWL_DATA_STORAGE_PATH)
