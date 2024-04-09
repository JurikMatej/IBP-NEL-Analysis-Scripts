#!/usr/bin/env python3

import asyncio
import tqdm
import sys
import logging
import re
from typing import List

import playwright._impl._errors as playwright_errors
from playwright.async_api import async_playwright
import pandas as pd

from src import crawling_utils
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry
from src.crawling_utils import ResponseData

###############################
# CONFIGURE THESE BEFORE USE: #
###############################
PLAYWRIGHT_GOTO_TIMEOUT = 5000  # ms

CRAWL_DOMAINS_FILEPATH = "analyze_realtime_eligible_domains.parquet"
CRAWL_PAGES_PER_DOMAIN = 10

CRAWL_ASYNC_WORKERS = 25


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


async def crawl_task(domain_queue: List[str], progressbar: tqdm.tqdm) -> CrawledDomainNelRegistry:
    task_registry = CrawledDomainNelRegistry()

    async with async_playwright() as pw:
        chromium = pw.chromium
        browser = await chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        while len(domain_queue) > 0:
            domain = domain_queue.pop(0)
            domain_link = f"https://{domain}/"

            url_domain_pattern = re.compile(fr"https?://({domain}/)")
            response_dataset = []

            visited_links = []
            link_queue = [domain_link]  # First request to a domain

            while len(link_queue) > 0:
                domain_next_link = link_queue.pop(0)

                # Prepare to intercept responses for current domain (set the callback for each domain)
                page.on("response", lambda response: response_dataset.append(
                    ResponseData(url=response.url, status=response.status, headers=response.headers))
                )

                try:
                    await page.goto(domain_next_link, timeout=0)  # PLAYWRIGHT_GOTO_TIMEOUT

                except playwright_errors.TimeoutError:
                    logger.warning(f"URL {domain_next_link} timed out")

                except playwright_errors.Error as error:
                    logger.warning(f"URL {domain_next_link} could not be retrieved: {error.message}")

                else:
                    visited_links.append(domain_next_link)
                    if len(visited_links) >= CRAWL_PAGES_PER_DOMAIN:
                        break  # Onto the next domain

                    link_queue = await crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

                    # Add the requested domain link and all it's unique sub-resources to the task registry
                    for domain_response_data in response_dataset:
                        # Process only resources originating from the crawled domain itself
                        if url_domain_pattern.match(domain_response_data.url):
                            task_registry.insert(domain, domain_response_data)

            # Current domain crawl finish
            progressbar.update()

        await ctx.close()
        await browser.close()

        return task_registry


async def main():
    # Load the domains to be crawled
    logger.info("Reading list of domains to crawl")

    eligible_domains = pd.read_parquet(CRAWL_DOMAINS_FILEPATH, columns=['url_domain']).reset_index(drop=True)
    domains = eligible_domains[eligible_domains.index < 100]['url_domain'].tolist()

    # Distribute async workload
    if len(domains) < CRAWL_ASYNC_WORKERS:
        domains_per_task = 1
        tasks_to_create = len(domains)
    else:
        domains_per_task = len(domains) // CRAWL_ASYNC_WORKERS
        tasks_to_create = CRAWL_ASYNC_WORKERS

    logger.info(f"Beginning to crawl {len(domains)} domains")

    result_registry = CrawledDomainNelRegistry()

    with tqdm.tqdm(total=len(domains)) as progressbar:
        tasks = []
        for i in range(tasks_to_create):
            if i != tasks_to_create - 1:
                # Distribute by partition to tasks
                tasks.append(crawl_task(domains[i * domains_per_task: (i + 1) * domains_per_task], progressbar))
            else:
                # Distribute the rest to the last task
                tasks.append(crawl_task(domains[i * domains_per_task:], progressbar))

        try:
            task_registries = await asyncio.gather(*tasks)
        except Exception as ex:
            logger.exception(f"Could not gather crawl task result \n{ex}")

    logger.info("Crawl completed! ...Collecting results")
    for task_registry in task_registries:
        result_registry.concat_content(task_registry)

    logger.info("Saving results")

    result_registry.count_totals()
    result_registry.filter_out_incorrect_nel()
    result_registry.save("test.html")

    logger.info("All done. Exiting...")


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())

    loop.close()
