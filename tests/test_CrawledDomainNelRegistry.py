from pandas import DataFrame

from tests.fixtures.nel_data import (
    domain_hosted_resources_3domains_of_total_15resources,
    domain_hosted_resources_3domains_10res_with_5res_without_nel,
    crawled_resources_10x_with_5x_without_nel,
    crawled_domain_resources_with_inconsistently_present_nel,
    crawled_resources_10x_correct_5x_incorrect_nel,
    crawled_domain_resources_with_inconsistently_correct_nel
)
from src.classes.CrawledDomainNelRegistry import CrawledDomainNelRegistry


class TestCrawledDomainNelRegistry:
    @staticmethod
    def setup_registry(data_fixture: DataFrame):
        registry = CrawledDomainNelRegistry()
        registry._data = data_fixture.copy()
        return registry

    def test_calculate_url_domain_hosted_resources(self, domain_hosted_resources_3domains_of_total_15resources):
        registry = self.setup_registry(domain_hosted_resources_3domains_of_total_15resources)

        calc_result = registry._calculate_url_domain_hosted_resources()

        # totals
        assert 15 == len(calc_result)           # 15 total resources
        assert 3 == calc_result.unique().size   # 3 total domains

        # domain 1
        assert 1 == len(calc_result.loc[0:9].unique())  # 1st domain has the same number of resources per data row
        assert 10 == calc_result.loc[0:9].unique()[0]   # 1st domain has 10 resources

        # domain 2
        assert 1 == len(calc_result.loc[10:12].unique())  # 2nd domain has the same number of resources per data row
        assert 3 == calc_result.loc[10:12].unique()[0]    # 2nd domain has 3 resources

        # domain 3
        assert 1 == len(calc_result.loc[13:].unique())  # 3rd domain has the same number of resources per data row
        assert 2 == calc_result.loc[13:].unique()[0]    # 3rd domain has 2 resources

    def test_calculate_url_domain_hosted_resources_with_nel(self,
                                                            domain_hosted_resources_3domains_10res_with_5res_without_nel):
        registry = self.setup_registry(domain_hosted_resources_3domains_10res_with_5res_without_nel)

        calc_result = registry._calculate_url_domain_hosted_resources_with_nel()

        assert 15 == len(calc_result)           # 15 total resources
        assert 3 == calc_result.unique().size   # 3 total domains

        assert 1 == len(calc_result.loc[0:9].unique())  # 1st domain has the same number of resources per data row
        assert 6 == calc_result.loc[0:9].unique()[0]    # 1st domain has 6 resources with NEL

        assert 1 == len(calc_result.loc[10:12].unique())  # 2nd domain has the same number of resources per data row
        assert 3 == calc_result.loc[10:12].unique()[0]    # 2nd domain has 3 resources with NEL

        assert 1 == len(calc_result.loc[13:].unique())  # 3rd domain has the same number of resources per data row
        assert 1 == calc_result.loc[13:].unique()[0]    # 3rd domain has 1 resource with NEL

    def test_calculate_total_crawled_resources_with_nel(self, crawled_resources_10x_with_5x_without_nel):
        registry = self.setup_registry(crawled_resources_10x_with_5x_without_nel)

        assert 10 == registry._calculate_total_crawled_resources_with_nel()

    def test_calculate_total_crawled_domains_with_nel(self, crawled_domain_resources_with_inconsistently_present_nel):
        registry = self.setup_registry(crawled_domain_resources_with_inconsistently_present_nel)

        assert 2 == registry._calculate_total_crawled_domains_with_nel()

    def test_calculate_total_crawled_resources_with_correct_nel(self, crawled_resources_10x_correct_5x_incorrect_nel):
        registry = self.setup_registry(crawled_resources_10x_correct_5x_incorrect_nel)

        assert 10 == registry._calculate_total_crawled_resources_with_correct_nel()

    def test_calculate_total_crawled_domains_with_correct_nel(self,
                                                              crawled_domain_resources_with_inconsistently_correct_nel):
        registry = self.setup_registry(crawled_domain_resources_with_inconsistently_correct_nel)

        assert 2 == registry._calculate_total_crawled_domains_with_correct_nel()
