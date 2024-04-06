from pandas import DataFrame
import pytest


@pytest.fixture()
def domain_hosted_resources_3domains_of_total_15resources():
    """
    3 total domains
    15 total resources

    1st domain: example.com = 10 resources
    2nd domain: test.com    =  3 resources
    3rd domain: hello.com   =  2 resources
    """

    urls = [
        *(f"https://example.com/{n}" for n in range(1, 11)),
        *(f"https://test.com/{n}" for n in range(1, 4)),
        *(f"https://hello.com/{n}" for n in range(1, 3))
    ]

    url_domains = [
        *(['example.com'] * 10),
        *(['test.com'] * 3),
        *(['hello.com'] * 2)
    ]

    return DataFrame({
        'url': urls,
        'url_domain': url_domains,
    })


@pytest.fixture()
def domain_hosted_resources_3domains_10res_with_5res_without_nel():
    """
    3 total domains
    15 total resources

    1st domain: example.com = 10 resources (6 with NEL, 4 without NEL)
    2nd domain: test.com    =  3 resources (3 with NEL)
    3rd domain: hello.com   =  2 resources (1 with NEL, 1 without NEL)
    """
    url_domains = [
        *(['example.com'] * 10),
        *(['test.com'] * 3),
        *(['hello.com'] * 2)
    ]

    nel_max_ages = [
        *(['3600'] * 6), *([None] * 4),     # 5 resources with NEL, 5 without NEL
        *(['7200'] * 3),                    # 3 resources with NEL
        '14400', None                       # 1  resource with NEL, 1 without NEL
    ]

    return DataFrame({
        'url_domain': url_domains,
        'nel_max_age': nel_max_ages
    })


@pytest.fixture()
def crawled_resources_10x_with_5x_without_nel():
    """
    10x nel_max_age filled with a valid value
     5x nel_max_age filled with None
    """
    return DataFrame({
        'nel_max_age': [
            *(["3600"] * 10),
            *([None] * 5)
        ]
    })


@pytest.fixture()
def crawled_domain_resources_with_inconsistently_present_nel():
    """
    3x domain
        1st: 0/5 resources with NEL
        2nd: 5/5 resources with NEL
        3rd: 3/5 resources with NEL
    """
    url_domains = [
        *(['example.com'] * 5),
        *(['test.com'] * 5),
        *(['hello.com'] * 5)
    ]

    nel_max_ages = [
        *([None] * 5),                  # example.com (0/5)
        *(["3600"] * 5),                # test.com    (5/5)
        *(["3600"] * 3), *([None] * 2)  # hello.com   (3/5)
    ]

    data = DataFrame({
        'url_domain': [*url_domains],
        'nel_max_age': [*nel_max_ages]
    })

    return data


@pytest.fixture()
def crawled_resources_10x_correct_5x_incorrect_nel():
    """
    10x resource with CORRECT NEL
     5x resource with INCORRECT NEL
    """
    return DataFrame({
        'nel_report_to': [
            *(['report.domain'] * 10),
            *([None] * 5)
        ]
    })


@pytest.fixture()
def crawled_domain_resources_with_inconsistently_correct_nel():
    """
    3x domain
        1st: 0/5 resources with CORRECT NEL
        2nd: 5/5 resources with CORRECT NEL
        3rd: 3/5 resources with CORRECT NEL
    """
    url_domains = [
        *(['example.com'] * 5),
        *(['test.com'] * 5),
        *(['hello.com'] * 5)
    ]

    nel_report_tos = [
        *([None] * 5),                           # example.com (0/5)
        *(["report.domain"] * 5),                # test.com    (5/5)
        *(["report.domain"] * 3), *([None] * 2)  # hello.com   (3/5)
    ]

    data = DataFrame({
        'url_domain': [*url_domains],
        'nel_report_to': [*nel_report_tos]
    })

    return data
