import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from playwright.sync_api import Response

from src import crawling_utils
from src.crawling_utils import NelHeaders, RtHeaders


class CrawledDomainNelRegistry(object):
    DF_SCHEMA = {
        'type': "object",
        'status': "UInt16",
        'url': "object",
        'url_domain': "category",
        'url_domain_hosted_resources': "UInt32",
        'url_domain_hosted_resources_with_nel': "UInt32",
        'url_domain_monitored_resources_ratio': "UInt32",
        'total_crawled_resources': "UInt32",
        'total_crawled_domains': "UInt32",
        'total_crawled_resources_with_nel': "UInt32",
        'total_crawled_domains_with_nel': "UInt32",
        'total_crawled_resources_with_correct_nel': "UInt32",
        'total_crawled_domains_with_correct_nel': "UInt32",
        'nel_max_age': "object",
        'nel_failure_fraction': "object",
        'nel_success_fraction': "object",
        'nel_include_subdomains': "object",
        'nel_report_to': "object",
        'rt_collectors': "object",
    }

    def __init__(self):
        self._total_crawled_resources: int = 0
        self._data: DataFrame = DataFrame({
            'type': [],
            'status': [],
            'url': [],
            'url_domain': [],
            'url_domain_hosted_resources': [],
            'url_domain_hosted_resources_with_nel': [],
            'url_domain_monitored_resources_ratio': [],
            'total_crawled_resources': [],
            'total_crawled_domains': [],
            'total_crawled_resources_with_nel': [],
            'total_crawled_domains_with_nel': [],
            'total_crawled_resources_with_correct_nel': [],
            'total_crawled_domains_with_correct_nel': [],
            'nel_max_age': [],
            'nel_failure_fraction': [],
            'nel_success_fraction': [],
            'nel_include_subdomains': [],
            'nel_report_to': [],
            'rt_collectors': [],
        }).astype(CrawledDomainNelRegistry.DF_SCHEMA)

    def insert(self, domain_name: str, response: Response):
        given_domain_rows = self._data[self._data['url_domain'] == domain_name]
        if not given_domain_rows[given_domain_rows['url'] == response.url].empty:
            # Do not process duplicate resources at all
            # Duplicate resources are mostly crawled when loading sub-resources of the currently crawled document
            return

        # If inserting a new resource (url), increase the number of total (unique) crawled resources
        self._total_crawled_resources += 1

        nel_fields: NelHeaders = crawling_utils.parse_nel_header(response.header_value("Nel"))
        rt_fields: RtHeaders = crawling_utils.parse_rt_header(response.header_value("Report-To"))
        content_type = crawling_utils.parse_content_type(response.header_value("Content-Type"))

        # Determine whether the NEL config for the current resource is correct or not
        if nel_fields.report_to is not None and nel_fields.report_to == rt_fields.group:
            # Correct NEL config
            nel_report_to = nel_fields.report_to
        else:
            # Incorrect NEL config
            nel_report_to = None

        new_registry_row = DataFrame({
            "type": [content_type],
            "status": [response.status],
            "url": [response.url],
            "url_domain": [domain_name],
            'url_domain_hosted_resources': [np.nan],
            'url_domain_hosted_resources_with_nel': [np.nan],
            'url_domain_monitored_resources_ratio': [np.nan],
            'total_crawled_resources': [np.nan],
            'total_crawled_domains': [np.nan],
            'total_crawled_resources_with_nel': [np.nan],
            'total_crawled_domains_with_nel': [np.nan],
            'total_crawled_resources_with_correct_nel': [np.nan],
            'total_crawled_domains_with_correct_nel': [np.nan],
            "nel_max_age": [nel_fields.max_age],
            "nel_failure_fraction": [nel_fields.failure_fraction],
            "nel_success_fraction": [nel_fields.success_fraction],
            "nel_include_subdomains": [nel_fields.include_subdomains],
            "nel_report_to": [nel_report_to],
            "rt_collectors": [rt_fields.endpoints]
        }).astype(CrawledDomainNelRegistry.DF_SCHEMA)

        # Append to registry dataframe
        if self._data.empty:
            self._data = new_registry_row
        else:
            self._data = pd.concat([self._data, new_registry_row])

    def save(self, file_path: str):
        if self._should_count_totals():
            self._count_totals()

        # self._registry.to_parquet(file_path)
        self._data.to_html(file_path)

    def _should_count_totals(self):
        if self._data[[
            'url_domain_hosted_resources',
            'url_domain_hosted_resources_with_nel',
            'url_domain_monitored_resources_ratio',
            'total_crawled_resources',
            'total_crawled_domains',
            'total_crawled_resources_with_nel',
            'total_crawled_domains_with_nel',
            'total_crawled_resources_with_correct_nel',
            'total_crawled_domains_with_correct_nel'
        ]].isnull().values.any():
            return True
        return False

    def _count_totals(self):
        self._data['url_domain_hosted_resources'] = self._calculate_url_domain_hosted_resources()
        self._data['url_domain_hosted_resources_with_nel'] = self._calculate_url_domain_hosted_resources_with_nel()

        self._data['url_domain_monitored_resources_ratio'] = (
            self._data['url_domain_hosted_resources_with_nel']
            / self._data['url_domain_hosted_resources']
            * 100
        )

        self._data['total_crawled_resources'] = self._total_crawled_resources
        self._data['total_crawled_domains'] = len(self._data['url_domain'].unique())

        self._data['total_crawled_resources_with_nel'] = self._calculate_total_crawled_resources_with_nel()
        self._data['total_crawled_domains_with_nel'] = self._calculate_total_crawled_domains_with_nel()

        self._data['total_crawled_resources_with_correct_nel'] = \
            self._calculate_total_crawled_resources_with_correct_nel()

        self._data['total_crawled_domains_with_correct_nel'] = \
            self._calculate_total_crawled_domains_with_correct_nel()

    def _calculate_url_domain_hosted_resources(self):
        url_counts_by_domain = self._data.groupby(['url_domain'])['url'].count()
        return self._data['url_domain'].map(url_counts_by_domain)

    def _calculate_url_domain_hosted_resources_with_nel(self):
        url_with_nel_counts_by_domain = self._data.groupby(['url_domain'])['nel_max_age'].count()
        return self._data['url_domain'].map(url_with_nel_counts_by_domain)

    def _calculate_total_crawled_resources_with_nel(self):
        return len(self._data[self._data['nel_max_age'].notna()])

    def _calculate_total_crawled_domains_with_nel(self):
        return self._data.groupby(['url_domain'])['nel_max_age'].agg(self.__agg_at_least_one_non_null).sum()

    def _calculate_total_crawled_resources_with_correct_nel(self):
        return len(self._data[self._data['nel_report_to'].notna()])

    def _calculate_total_crawled_domains_with_correct_nel(self):
        return self._data.groupby(['url_domain'])['nel_report_to'].agg(self.__agg_at_least_one_non_null).sum()

    @staticmethod
    def __agg_at_least_one_non_null(nel_report_to: Series):
        contains_any_correct_nel_resources = \
            (nel_report_to
             .where(nel_report_to.notna(), 0)
             .where(~nel_report_to.notna(), 1)
             .sum() > 0)

        return 1 if contains_any_correct_nel_resources else 0
