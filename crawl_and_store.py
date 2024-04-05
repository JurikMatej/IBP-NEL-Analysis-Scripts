#!/usr/bin/env python3

import sys
import logging
import re

import playwright._impl._errors as playwright_errors
from playwright.sync_api import sync_playwright, Response
import pandas as pd

from src import crawling_utils
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry


###############################
# CONFIGURE THESE BEFORE USE: #
###############################
PLAYWRIGHT_GOTO_TIMEOUT = 0  # ms (TODO set to a reasonable number - for now disabled for debugging)

CRAWL_PAGES_PER_DOMAIN = 10


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


# Globals
# TODO rewrite to parallel state
eligible_domains = (pd.read_parquet("analyze_realtime_eligible_domains.parquet", columns=['url_domain'])
                    .reset_index(drop=True))

# crawled_domains_indexer = CrawledDomainIndexer()
result_registry = CrawledDomainNelRegistry()


def response_intercept(response: Response, domain_name: str):
    url_domain_pattern = re.compile(fr"https?://({domain_name}/)")

    if url_domain_pattern.match(response.url):
        # Process only resources originating from the crawled domain itself
        result_registry.insert(domain_name, response)


def main():
    global eligible_domains, result_registry  # , crawled_domains_indexer

    test_eligible_domains = eligible_domains[(eligible_domains.index > 3040) & (eligible_domains.index < 3044)]
    # crawled_domains_indexer.set_index(test_eligible_domains.index[0])

    with sync_playwright() as pw:
        chromium = pw.chromium
        browser = chromium.launch(headless=True)
        ctx = browser.new_context()

        page = ctx.new_page()

        for domain in test_eligible_domains['url_domain']:
            link_queue = []
            domain_link = f"https://{domain}/"

            # Prepare to intercept responses for current domain
            page.on("response", lambda response: response_intercept(response, domain))

            # First request to a domain
            try:
                page.goto(domain_link, timeout=PLAYWRIGHT_GOTO_TIMEOUT)

            except playwright_errors.TimeoutError:
                logger.warning(f"URL {domain_link} timed out")

            except playwright_errors.Error as error:
                logger.warning(f"URL {domain_link} could not be retrieved: {error.name} | {error.message}")

            visited_links = [domain_link]
            link_queue = crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

            while len(link_queue) > 0:
                next_link = link_queue.pop(0)

                # All subsequent requests to sub-pages
                try:
                    page.goto(domain_link, timeout=PLAYWRIGHT_GOTO_TIMEOUT)

                except playwright_errors.TimeoutError:
                    logger.warning(f"URL {domain_link} timed out")

                except playwright_errors.Error as error:
                    logger.warning(f"URL {domain_link} could not be retrieved: {error.name} | {error.message}")

                visited_links.append(next_link)
                if len(visited_links) >= CRAWL_PAGES_PER_DOMAIN:
                    break  # Onto the next domain

                link_queue = crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

        ctx.close()
        browser.close()

    result_registry.save("test.html")

    logger.info("All done. Exiting...")


if __name__ == '__main__':
    main()
