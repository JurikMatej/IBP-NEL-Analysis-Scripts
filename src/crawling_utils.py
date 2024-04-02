from typing import List

from playwright.sync_api import Page


def update_link_queue(link_queue: List[str], visited_links: List[str], current_domain_name: str,
                      page: Page) -> List[str]:
    new_unique_links = _get_unique_page_links(page)
    links_to_the_same_domain = _filter_selfpointing_and_external_links(new_unique_links, current_domain_name)
    valid_links_to_the_same_domain = \
        _relative_links_to_absolute_links(links_to_the_same_domain, current_domain_name)
    valid_new_links = list(filter(lambda link: link not in visited_links, valid_links_to_the_same_domain))

    link_queue.extend(valid_new_links)
    result = list(set(link_queue))

    return result


def _get_unique_page_links(page: Page) -> List[str]:
    links = [a.get_attribute("href") for a in page.locator("css=a").all() if a.get_attribute("href") is not None]
    return list(set(links))


def _filter_selfpointing_and_external_links(links: List[str], current_domain_name: str) -> List[str]:
    return list(filter(
        lambda link:
            link.startswith(f"https://{current_domain_name}/") or link.startswith("/"),
        links))


def _relative_links_to_absolute_links(links_to_the_same_domain: List[str], current_domain_name: str) -> List[str]:
    return list(map(lambda link: __relative_to_absolute_link(link, current_domain_name), links_to_the_same_domain))


def __relative_to_absolute_link(link: str, current_domain_name: str) -> str:
    if link.startswith("/"):
        result = f"https://{current_domain_name}{link}"
    else:
        result = link

    if not result.endswith("/"):
        result = f"{result}/"

    return result
