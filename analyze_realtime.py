#!/usr/bin/env python3

import sys
import logging
import re

from playwright.sync_api import sync_playwright, Playwright, Response
import pandas as pd

from src.classes.CrawledDomainIndexer import CrawledDomainIndexer
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


# Globals
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
        result_registry.insert(domain_name,
                               response.url,
                               response.header_values('Nel'),
                               response.header_value('Report-To'))


def main():
    global eligible_domains, crawled_domains_indexer, result_registry

    test_eligible_domains = eligible_domains[(eligible_domains.index > 1040) & (eligible_domains.index < 1050)]
    crawled_domains_indexer.set_index(test_eligible_domains.index[0])

    with sync_playwright() as pw:
        chromium = pw.chromium
        browser = chromium.launch(headless=True)
        ctx = browser.new_context()

        page = ctx.new_page()
        page.on("response", response_intercept)

        for domain in test_eligible_domains['url_domain']:
            page.goto(f"https://{domain}/")

            # TODO Make it crawl links
            crawled_domains_indexer.next_index()

        ctx.close()
        browser.close()

    result = result_registry.get_content()
    print()


if __name__ == '__main__':
    main()
