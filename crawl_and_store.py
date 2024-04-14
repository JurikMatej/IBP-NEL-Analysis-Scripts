#!/usr/bin/env python3

"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

description:    Descriptive..

purpose:        Purposeful...
"""

import gc
import logging
import re
import sys
from pathlib import Path
from typing import List

import asyncio
import pandas as pd
import playwright._impl._errors as playwright_errors
import tqdm
from playwright.async_api import async_playwright, Playwright

from merge_crawled_and_save import merge_crawled_and_save
from src import crawling_utils
from src.classes.DomainNelDataRegistry import DomainNelDataRegistry
from src.classes.DomainLinkTree import DomainLinkTree
from src.crawling_utils import ResponseData


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='\n%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


###############################
# CONFIGURE THESE BEFORE USE: #
###############################
CRAWL_DATA_RAW_STORAGE_PATH = "data/crawled_raw/blobs"
CRAWL_DATA_STORAGE_PATH = "data/crawled_raw"

CRAWL_DOMAINS_LIST_FILEPATH = "data/domains_to_crawl.parquet"
CRAWL_PAGES_PER_DOMAIN = 20

CRAWL_ASYNC_WORKERS = 6
CRAWL_ASYNC_WORKER_LIFETIME_WORKLOAD = 40

CRAWL_ASYNC_PAGE_LOAD_FAILSAFE_TIMEOUT = 120_000  # milliseconds


def crash_logger_callback(domain, exception):
    logger.warning(f"Domain ({domain}) crawl failed: {exception}")


def noop_logger_callback(_):
    pass


async def crawl_task(pw: Playwright, domain_queue: List[str], progressbar: tqdm.tqdm):
    chromium = pw.chromium
    browser = await chromium.launch(headless=True)
    ctx = await browser.new_context()
    ctx.on("weberror", noop_logger_callback)

    while len(domain_queue) > 0:
        # Domain vars
        domain_name = domain_queue.pop(0)
        domain_url_pattern = re.compile(fr"https?://{domain_name}")

        # Check if the domain data is already crawled
        path_to_save = Path(f"{CRAWL_DATA_RAW_STORAGE_PATH}/{domain_name}.parquet")
        if path_to_save.exists():
            logger.warning(f"SKIP - Domain data already crawled in '{CRAWL_DATA_RAW_STORAGE_PATH}' ({domain_name})")
            progressbar.update()
            continue

        # Prepare to intercept responses for current domain (set the callback for each domain)
        domain_response_dataset = []
        page = await ctx.new_page()
        page.on("response", lambda response: domain_response_dataset.append(
            ResponseData(url=response.url, status=response.status, headers=response.headers))
        )
        # Also log page crashes
        page.on('crash', lambda ex: crash_logger_callback(domain_name, ex))

        # Crawling infrastructure for the current domain
        domain_nel_data_registry = DomainNelDataRegistry()
        domain_link_tree = DomainLinkTree(domain_name)
        domain_next_link = domain_link_tree.get_next()  # First request will be to the domain itself

        while domain_next_link is not None:
            fetch_failed_totally = False
            try:
                await page.goto(domain_next_link, timeout=CRAWL_ASYNC_PAGE_LOAD_FAILSAFE_TIMEOUT)

            except playwright_errors.TimeoutError:
                # This will probably happen quite often, but strategically it is better to not wait for large
                # sub-resources for every page
                # Keeping only partial data = direct response for the requested link + the sub-resources fetched before timeout
                logger.warning(f"FULL_LOAD_TIMED_OUT - Full load took too long. Keeping only partial data ({domain_next_link})")

            except playwright_errors.Error as error:
                logger.warning(f"URL fetch failed: {error.message.split('\n')[0] or error.name}")
                fetch_failed_totally = True

            if not fetch_failed_totally:
                # Update NEL data registry with response dataset
                for domain_response_data in domain_response_dataset:
                    # Process only response resources originating from the crawled domain itself
                    if domain_url_pattern.match(domain_response_data.url):
                        domain_nel_data_registry.insert(domain_name, domain_response_data)

                # Stop crawling the domain if the per-domain quota was reached
                if domain_link_tree.get_visited_links_count() > CRAWL_PAGES_PER_DOMAIN:
                    break  # Onto the next domain

                # Add new links to the link tree & continue crawling
                new_links_available = await crawling_utils.get_formatted_page_links(page, domain_name)
                domain_link_tree.add(new_links_available)

                # Reset inserted data before crawling next link
                domain_response_dataset.clear()

            domain_next_link = domain_link_tree.get_next()

        # Current DOMAIN finished crawling
        domain_nel_data_registry.save_raw(path_to_save)
        progressbar.update()

        # Make sure not to hold on to any redundant data that uses RAM
        await page.close()
        await ctx.clear_cookies()
        del domain_nel_data_registry
        del domain_link_tree
        del domain_next_link
        gc.collect()

    # Crawler task finished the whole domain workload
    await ctx.close()
    await browser.close()
    gc.collect()


def domain_workload_generator(domains: List[str]):
    generated_index_range = CRAWL_ASYNC_WORKER_LIFETIME_WORKLOAD

    for index_range_start in range(0, len(domains), generated_index_range):
        if (index_range_start + generated_index_range) <= len(domains) - 1:
            yield domains[index_range_start:index_range_start + generated_index_range]
        else:
            yield domains[index_range_start:]

    while True:
        # Keep generating no workload infinitely when all the domains have already been generated
        yield []


async def main():
    crawl_data_raw_path = Path(CRAWL_DATA_RAW_STORAGE_PATH)
    crawl_data_raw_path.mkdir(parents=True, exist_ok=True)

    crawl_data_output_path = Path(CRAWL_DATA_STORAGE_PATH)
    crawl_data_output_path.mkdir(parents=True, exist_ok=True)

    logger.info("Reading list of domains to crawl")

    eligible_domains_source = Path(CRAWL_DOMAINS_LIST_FILEPATH)
    if not eligible_domains_source.exists() or not eligible_domains_source.is_file():
        logger.error("Eligible domains dataframe file not found. Please generate it using the eligible domains script")
        return

    eligible_domains = pd.read_parquet(CRAWL_DOMAINS_LIST_FILEPATH, columns=['url_domain']).reset_index(drop=True)
    # domains = \
    #     eligible_domains[(eligible_domains.index >= 16840) & (eligible_domains.index < 30000)]['url_domain'].tolist()
    domains = eligible_domains['url_domain'].tolist()
    domain_workload_pool = domain_workload_generator(domains)

    logger.info(f"Beginning to crawl {len(domains)} domains")

    async with async_playwright() as pw:
        with tqdm.tqdm(total=len(domains)) as progressbar:
            pending = set()
            for i in range(CRAWL_ASYNC_WORKERS):
                task_domain_pool = next(domain_workload_pool)
                if len(task_domain_pool) > 0:
                    pending.add(asyncio.create_task(crawl_task(pw, task_domain_pool, progressbar)))
                else:
                    break

            try:
                should_generate_more_tasks = True
                while pending:
                    done, pending = await asyncio.wait(
                        pending,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    logger.debug("A task has been completed and closed")

                    if should_generate_more_tasks:
                        next_task_domain_pool = next(domain_workload_pool)
                        if len(next_task_domain_pool) > 0:
                            logger.debug("Spawning a new Task")
                            pending.add(asyncio.ensure_future(crawl_task(pw, next_task_domain_pool, progressbar)))
                        else:
                            should_generate_more_tasks = False

                    # crawling_utils.log_all_tasks_stack_trace()

            except Exception as ex:
                logger.exception(f"Error occurred during the async crawl: \n{ex}")

    logger.info("Crawl completed - gathering the results")
    merge_crawled_and_save(CRAWL_DATA_RAW_STORAGE_PATH, CRAWL_DATA_STORAGE_PATH)

    logger.info("All done. Exiting...")


if __name__ == '__main__':
    event_loop = asyncio.new_event_loop()
    event_loop.run_until_complete(main())

    event_loop.close()
