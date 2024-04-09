from dataclasses import dataclass

import re
import sys
import logging
from typing import List
from playwright.async_api import Page
import playwright._impl._errors as playwright_errors


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class RtHeaders:
    group: str | None
    endpoints: list[str] | None


@dataclass
class NelHeaders:
    max_age: str | None
    failure_fraction: str | None
    success_fraction: str | None
    include_subdomains: str | None
    report_to: str | None


@dataclass
class ResponseData:
    url: str | None
    status: int | None
    headers: dict[str, str] | None


async def update_link_queue(link_queue: List[str], visited_links: List[str], current_domain_name: str,
                            page: Page) -> List[str]:
    available_unique_links = await _get_unique_page_links(page)
    links_to_current_domain = _filter_selfpointing_and_external_links(available_unique_links, current_domain_name)

    unique_document_links = _filter_fragment_and_query_string_links(links_to_current_domain)

    valid_links_to_the_same_domain = _relative_links_to_absolute_links(unique_document_links, current_domain_name)

    valid_new_links = list(filter(lambda link: link not in visited_links, valid_links_to_the_same_domain))

    link_queue.extend(valid_new_links)
    result = list(set(link_queue))

    return result


async def _get_unique_page_links(page: Page) -> List[str]:
    try:
        all_anchors = await page.eval_on_selector_all("a",
                                                      "elements => elements.map(element => element.href)")

    except playwright_errors.Error:
        logger.warning(f"Page {page.url}: could not extract anchor elements")
        return []

    return list(set(all_anchors))


def _filter_selfpointing_and_external_links(links: List[str], current_domain_name: str) -> List[str]:
    return list(filter(
        lambda link:
            link.startswith(f"https://{current_domain_name}/")
            or link.startswith(f"https://www.{current_domain_name}/")
            or link.startswith("/"),
        links))


def _filter_fragment_and_query_string_links(links_to_the_same_domain: List[str]):
    result = []

    for link in links_to_the_same_domain:
        query_sign_index = link.find('?')
        if query_sign_index != -1:
            link = link[:query_sign_index]

        last_slash_index = link.rfind('/')
        if last_slash_index != -1:
            fragment_index = link[last_slash_index + 1:].find('#')
            if fragment_index != -1:
                result.append(link[:last_slash_index + fragment_index + 1])
                continue

        result.append(link)

    return list(set(result))


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
    """
    This method does not return the real parsed values from content type headers. Rather, it tries to map
    received content types to HTTPArchive supported 'type' column values found during earlier analysis.

    HTTPArchive supported types so far:
        * audio
        * css
        * font
        * html
        * image
        * other
        * script
        * text
        * video
        * xml
    """
    if content_type_header is None or content_type_header.strip() == "":
        return ""

    if ';' in content_type_header:
        parsed = content_type_header.split(';')[0].strip()
    else:
        parsed = content_type_header.strip()

    if parsed.count('/') > 1:
        return ""

    category, specific = parsed.split('/')

    return _content_type_to_httparchive_type(category, specific)


def _content_type_to_httparchive_type(category, specific):
    # Override specific application types or default to 'other'
    if category == 'application':
        if specific.startswith('xml'):
            return 'xml'
        if specific == 'javascript':
            return 'script'

        return 'other'

    # Override specific text types or default to 'text'
    if category == 'text':
        if specific in ['css', 'html']:
            return specific
        elif specific == 'javascript':
            return 'script'

        return category

    # Lastly, if the category is any of these, return only the category name
    if category in ['video', 'audio', 'font', 'image']:
        return category

    # Anything else defaults to 'other' for now
    return 'other'


def parse_nel_header(nel_header: str | None) -> NelHeaders:
    if nel_header is None or nel_header.strip() == "":
        return NelHeaders(*([None] * 5))  # All fields = None

    ma_ptrn = re.compile(r".*max_age[\"\']\s*:\s*([0-9]+)")
    ma_match = ma_ptrn.search(nel_header)
    # If max_age is None, the resource did not return with NEL header
    # (max age is required without any default value fallback)
    # Therefore IF max_age is None THEN that resource is NOT NEL-monitored
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
