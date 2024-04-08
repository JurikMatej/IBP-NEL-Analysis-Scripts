#!/usr/bin/env python3

import asyncio
import tqdm
import sys
import logging
import re

import playwright._impl._errors as playwright_errors
from playwright.async_api import async_playwright
import pandas as pd

from src import crawling_utils
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry
from src.crawling_utils import ResponseData

###############################
# CONFIGURE THESE BEFORE USE: #
###############################
PLAYWRIGHT_GOTO_TIMEOUT = 0  # ms (TODO set to a reasonable number - for now disabled for debugging)

CRAWL_PAGES_PER_DOMAIN = 10


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


async def crawl_task(progressbar: tqdm.tqdm) -> CrawledDomainNelRegistry:
    task_registry = CrawledDomainNelRegistry()

    async with async_playwright() as pw:
        chromium = pw.chromium
        browser = await chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        while not domain_queue.empty():
            domain = await domain_queue.get()
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
                    await page.goto(domain_next_link, timeout=PLAYWRIGHT_GOTO_TIMEOUT)

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

    # TODO standardize input file & make it a constant
    eligible_domains = (pd.read_parquet("analyze_realtime_eligible_domains.parquet", columns=['url_domain'])
                        .reset_index(drop=True))
    test_eligible_domains = eligible_domains[(eligible_domains.index > 5000) & (eligible_domains.index < 5011)]
    domains = test_eligible_domains['url_domain']

    # Prepare domain name queue for the crawling tasks
    for domain in domains:
        domain_queue.put_nowait(domain)

    logger.info(f"Beginning to crawl {len(test_eligible_domains)} domains")

    result_registry = CrawledDomainNelRegistry()

    with tqdm.tqdm(total=len(domains), position=1) as progressbar:
        tasks = []
        for i in range(10):
            tasks.append(crawl_task(progressbar))

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
    domain_queue: asyncio.Queue = asyncio.Queue()
    asyncio.run(main())
