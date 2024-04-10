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
from playwright.async_api import async_playwright

from merge_crawled_and_save import merge_crawled_and_save
from src import crawling_utils
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry
from src.crawling_utils import ResponseData

###############################
# CONFIGURE THESE BEFORE USE: #
###############################
CRAWL_DATA_RAW_STORAGE_PATH = "crawl_data_raw/blobs"
CRAWL_DATA_STORAGE_PATH = "crawl_data_raw"

CRAWL_DOMAINS_FILEPATH = "crawl_and_store_eligible_domains.parquet"
CRAWL_PAGES_PER_DOMAIN = 10

CRAWL_ASYNC_WORKERS = 20
CRAWL_ASYNC_WORKER_LIFETIME_WORKLOAD = 20


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='\n%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


async def crawl_task(domain_queue: List[str], progressbar: tqdm.tqdm):
    async with async_playwright() as pw:
        chromium = pw.chromium
        browser = await chromium.launch(headless=True)  # TODO should launch one chrome and create many contexts ?
        ctx = await browser.new_context()               # TODO should create one context and create many pages ?

        def weberror_callback(e):
            # logger.warning(f"Web exception occurred (no impact on the crawling process): {e.error}")
            pass
        ctx.on("weberror", weberror_callback)

        page = await ctx.new_page()
        page.on('crash', weberror_callback)

        # Disable HTTP cache by enabling routes TODO just a possibility - check whether hinders performance
        # await page.route('**', lambda route: route.continue_())

        while len(domain_queue) > 0:
            domain = domain_queue.pop(0)

            path_to_save = Path(f"{CRAWL_DATA_RAW_STORAGE_PATH}/{domain}.parquet")
            if path_to_save.exists():
                logger.warning(f"SKIP - Domain data already crawled in '{CRAWL_DATA_RAW_STORAGE_PATH}' ({domain})")
                progressbar.update()
                continue

            domain_registry = CrawledDomainNelRegistry()
            domain_link = f"https://{domain}/"

            url_domain_pattern = re.compile(fr"https?://({domain}/)")

            # Prepare to intercept responses for current domain (set the callback for each domain)
            response_dataset = []
            page.on("response", lambda response: response_dataset.append(
                ResponseData(url=response.url, status=response.status, headers=response.headers))
            )

            visited_links = []
            link_queue = [domain_link]  # First request to a domain

            while len(link_queue) > 0:
                domain_next_link = link_queue.pop(0)

                try:
                    await page.goto(domain_next_link, timeout=0)

                except playwright_errors.TimeoutError:
                    logger.warning(f"URL {domain_next_link} timed out")

                except playwright_errors.Error as error:
                    logger.warning(f"URL fetch failed: {error.message.split('\n')[0] or error.name}")

                else:
                    visited_links.append(domain_next_link)
                    if len(visited_links) > CRAWL_PAGES_PER_DOMAIN:
                        break  # Onto the next domain

                    link_queue = await crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

                    # Add the requested domain link and all it's unique sub-resources to the task registry
                    for domain_response_data in response_dataset:
                        # Process only response resources originating from the crawled domain itself
                        if url_domain_pattern.match(domain_response_data.url):
                            domain_registry.insert(domain, domain_response_data)

            # Current domain crawl finish
            domain_registry.save_raw(path_to_save)
            progressbar.update()

            # Make sure not to hold on to any redundant data that uses RAM
            await ctx.clear_cookies()
            del domain_registry
            del response_dataset
            del visited_links
            del link_queue
            gc.collect()

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
    eligible_domains = pd.read_parquet(CRAWL_DOMAINS_FILEPATH, columns=['url_domain']).reset_index(drop=True)
    domains = eligible_domains[(eligible_domains.index >= 0) & (eligible_domains.index < 5000)]['url_domain'].tolist()
    domain_workload_pool = domain_workload_generator(domains)

    logger.info(f"Beginning to crawl {len(domains)} domains")

    with tqdm.tqdm(total=len(domains)) as progressbar:
        pending = set()
        for i in range(CRAWL_ASYNC_WORKERS):
            task_domain_pool = next(domain_workload_pool)
            if len(task_domain_pool) > 0:
                pending.add(asyncio.create_task(crawl_task(task_domain_pool, progressbar)))
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
                        pending.add(asyncio.ensure_future(crawl_task(next_task_domain_pool, progressbar)))
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
