#!/usr/bin/env python3

import sys
import logging
import re

from playwright.sync_api import sync_playwright, Response
import pandas as pd

from src import crawling_utils
from src.classes.CrawledDomainIndexer import CrawledDomainIndexer
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry


# Constants
PLAYWRIGHT_GOTO_TIMEOUT = 10_000  # ms


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


# Globals
# TODO rewrite to parallel state
eligible_domains = (pd.read_parquet("analyze_realtime_eligible_domains.parquet", columns=['url_domain'])
                    .reset_index(drop=True))

crawled_domains_indexer = CrawledDomainIndexer(0)
result_registry = CrawledDomainNelRegistry()


def response_intercept(response: Response):
    global eligible_domains, crawled_domains_indexer, result_registry

    domain = eligible_domains.iloc[crawled_domains_indexer.get_index()]
    domain_name = domain['url_domain']

    url_domain_pattern = re.compile(fr"https?://({domain_name}/)")
    if url_domain_pattern.match(response.url):
        # TODO insert every URL only once
        result_registry.insert(domain_name,
                               response.url,
                               response.header_values('Nel'),
                               response.header_value('Report-To'))


def main():
    global eligible_domains, crawled_domains_indexer, result_registry

    test_eligible_domains = eligible_domains[(eligible_domains.index > 1040) & (eligible_domains.index < 1042)]
    crawled_domains_indexer.set_index(test_eligible_domains.index[0])

    with sync_playwright() as pw:
        chromium = pw.chromium
        browser = chromium.launch(headless=True)
        ctx = browser.new_context()

        page = ctx.new_page()
        page.on("response", response_intercept)

        for domain in test_eligible_domains['url_domain']:
            link_queue = []
            domain_link = f"https://{domain}/"
            visited_links = [domain_link]

            # First request to a domain
            page.goto(domain_link, timeout=PLAYWRIGHT_GOTO_TIMEOUT)
            link_queue = crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

            while len(link_queue) > 0:
                next_link = link_queue.pop(0)
                visited_links.append(next_link)

                page.goto(next_link, timeout=PLAYWRIGHT_GOTO_TIMEOUT)
                link_queue = crawling_utils.update_link_queue(link_queue, visited_links, domain, page)

            crawled_domains_indexer.next_index()

        ctx.close()
        browser.close()

    # TODO process results and store in the same way HttpArchive metrics are made
    result = result_registry.get_content()
    print()


if __name__ == '__main__':
    main()
