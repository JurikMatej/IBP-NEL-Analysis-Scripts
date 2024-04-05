import re
from typing import List
from collections import namedtuple
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


def parse_content_type(content_type_header: str | None) -> str:
    if content_type_header is None or content_type_header.strip() == "":
        return ""

    ct_pattern = re.compile(r"^.+/(.+);",)  # TODO FIX - Relies on trailing ';' char
    match = ct_pattern.search(content_type_header)
    if match:
        return match.group(1)
    return ""


NelHeaders = namedtuple("NelHeaders",
                        ['max_age', 'failure_fraction', 'success_fraction', 'include_subdomains', 'report_to']
                        )
RtHeaders = namedtuple("RtHeaders", ['group', 'endpoints'])


def parse_nel_header(nel_header: str | None) -> NelHeaders:
    if nel_header is None or nel_header.strip() == "":
        return NelHeaders(*([None] * 5))  # All fields = None

    ma_ptrn = re.compile(r".*max_age[\"\']\s*:\s*([0-9]+)")
    ma_match = ma_ptrn.search(nel_header)
    ma = ma_match.group(1) if ma_match else None

    ff_ptrn = re.compile(r".*failure[_]fraction[\"\']\s*:\s*([0-9.]+)")
    ff_match = ff_ptrn.search(nel_header)
    ff = ff_match.group(1) if ff_match else "1.0"
    if ff == "0":
        ff = "0.0"
    elif ff == "1":
        ff = "1.0"

    sf_ptrn = re.compile(r".*success[_]fraction[\"\']\s*:\s*([0-9.]+)")
    sf_match = sf_ptrn.search(nel_header)
    sf = sf_match.group(1) if sf_match else "0.0"
    if sf == "0":
        sf = "0.0"
    elif sf == "1":
        sf = "1.0"

    inc_subd_ptrn = re.compile(r".*include[_]subdomains[\"\']\s*:\s*(\w+)")
    inc_subd_match = inc_subd_ptrn.search(nel_header)
    inc_subd = inc_subd_match.group(1) if inc_subd_match else "false"

    rt_ptrn = re.compile(r".*report_to[\"\']\s*:\s*[\"\'](.+?)[\"\']")
    rt_match = rt_ptrn.search(nel_header)
    rt = rt_match.group(1) if rt_match else None

    return NelHeaders(ma, ff, sf, inc_subd, rt)


def parse_rt_header(rt_header: str | None) -> RtHeaders:
    if rt_header is None or rt_header.strip() == "":
        return RtHeaders(None, [])  # Missing values

    grp_ptrn = re.compile(r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']")
    grp_match = grp_ptrn.search(rt_header)
    grp = grp_match.group(1) if grp_match else "default"

    endpts_ptrn = re.compile(r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[/][\\]*?[/]([^/]+?)[\\]*?[/\"]",
                             re.MULTILINE)
    endpts = endpts_ptrn.findall(rt_header) or []

    return RtHeaders(grp, endpts)
