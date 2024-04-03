import pandas as pd
from pandas import DataFrame
from playwright.sync_api import Response

from src import crawling_utils
from src.crawling_utils import NelHeaders, RtHeaders


class CrawledDomainNelRegistry(object):

    DF_SCHEMA = {
        'type': "object",
        'status': "UInt16",
        'url': "object",
        'url_domain': "category",
        'nel_max_age': "object",
        'nel_failure_fraction': "object",
        'nel_success_fraction': "object",
        'nel_include_subdomains': "object",
        'nel_report_to': "object",
        'rt_collectors': "object",
    }

    def __init__(self):
        # TODO extend columns if necessary
        self._registry: DataFrame = DataFrame({
            'type': [],
            'status': [],
            'url': [],
            'url_domain': [],
            'nel_max_age': [],
            'nel_failure_fraction': [],
            'nel_success_fraction': [],
            'nel_include_subdomains': [],
            'nel_report_to': [],
            'rt_collectors': [],
        }).astype(CrawledDomainNelRegistry.DF_SCHEMA)

    def get_content(self):
        return self._registry

    def insert(self, domain_name: str, response: Response):
        """
        TODO
            url_domain_hosted_resources
            total_crawled_resources
        """

        given_domain_rows = self._registry[self._registry['url_domain'] == domain_name]

        if not given_domain_rows[given_domain_rows['url'] == response.url].empty:
            # Do not insert duplicate resources
            return

        nel_fields: NelHeaders = \
            crawling_utils.parse_nel_header(response.header_value("Nel")) if 'nel' in response.headers else None

        if nel_fields is None:
            # TODO do not insert is OK but increase total_resource_count
            return

        rt_fields: RtHeaders = crawling_utils.parse_rt_header(response.header_value("Report-To")) \
            if "report-to" in response.headers else None

        content_type = response.header_value("Content-Type") or ""

        _type = crawling_utils.parse_content_type(content_type)
        status = response.status
        url = response.url
        url_domain = domain_name
        nel_max_age = nel_fields.max_age
        nel_failure_fraction = nel_fields.failure_fraction
        nel_success_fraction = nel_fields.success_fraction
        nel_include_subdomains = nel_fields.include_subdomains

        # nel_report_to field
        if nel_fields.report_to != "" and nel_fields.report_to == rt_fields.group:
            # Correct NEL config
            nel_report_to = nel_fields.report_to
        else:
            # Incorrect NEL config
            nel_report_to = ""

        rt_collectors = rt_fields.endpoints

        new_registry_row = DataFrame({
            "type": [_type],
            "status": [status],
            "url": [url],
            "url_domain": [url_domain],
            "nel_max_age": [nel_max_age],
            "nel_failure_fraction": [nel_failure_fraction],
            "nel_success_fraction": [nel_success_fraction],
            "nel_include_subdomains": [nel_include_subdomains],
            "nel_report_to": [nel_report_to],
            "rt_collectors": [rt_collectors]
        }).astype(CrawledDomainNelRegistry.DF_SCHEMA)

        # Append to registry dataframe
        if self._registry.empty:
            self._registry = new_registry_row
        else:
            self._registry = pd.concat([self._registry, new_registry_row])
