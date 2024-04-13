from dataclasses import dataclass

import asyncio
from io import StringIO
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


async def get_formatted_page_links(page: Page, domain_name: str) -> List[str]:
    available_links = await _get_unique_page_links(page)
    internal_links = _filter_external_links(available_links, domain_name)
    unique_document_links = _filter_fragments_and_query_strings_from_links(internal_links)

    parsed_relative_links = _absolute_to_relative_links(unique_document_links, domain_name)

    result = list(set(parsed_relative_links))
    return result


async def _get_unique_page_links(page: Page) -> List[str]:
    try:
        all_anchors = await page.eval_on_selector_all("a",
                                                      "elements => elements.map(element => element.href)")
        return list(set(all_anchors))

    except playwright_errors.Error as ex:
        logger.warning(f"Page {page.url}: {ex}")
        return []

    except (TypeError, Exception):
        # Weird behavior - e.g. all_anchors evaluated as a list of dicts
        return []


def _filter_external_links(links: List[str], domain_name: str) -> List[str]:
    return list(filter(
        lambda link:
            link.startswith(f"https://{domain_name}/")
            or link.startswith("/"),
        links))


def _filter_fragments_and_query_strings_from_links(links: List[str]) -> List[str]:
    """
    Filter out fragments and query strings from the URL links provided.
    Effectively filters URL links pointing to the same location, only different sections of the content
    """
    result = []

    for link in links:
        # Temporarily remove the query string part of URL link if found
        query_sign_index = link.find('?')
        if query_sign_index != -1:
            link = link[:query_sign_index]

        # Find the last slash character
        last_slash_index = link.rfind('/')
        if last_slash_index != -1:
            # Check whether a hash (fragment) char is found after the slash
            fragment_index = link[last_slash_index + 1:].find('#')
            if fragment_index != -1:
                # If so, exclude the fragment part from the link
                link = link[:last_slash_index + fragment_index + 1]

        if link.endswith("/"):
            link = link[:-1]

        result.append(link)

    return list(set(result))


def _absolute_to_relative_links(links: List[str], domain_name: str) -> List[str]:
    return list(map(lambda link: _absolute_to_relative_link(link, domain_name), links))


def _absolute_to_relative_link(link: str, domain_name: str) -> str:
    result = re.sub("^https?://", "", link)

    if result.startswith("/"):
        result = result[1:]

    if result.endswith("/"):
        result = result[:-1]

    if result.startswith(domain_name):
        result = result[len(domain_name) + 1:]

    return result


def parse_content_type(content_type_header: str | None) -> str:
    """
    This method does not return the real parsed values from content type headers.
    Rather, it tries to map received content types to HTTPArchive supported 'type' column values
    found during earlier analysis.

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

    split = parsed.split('/')

    if len(split) > 1:
        category = split[0]
        specific = split[1]
    elif len(split) == 1:
        category = split[0]
        specific = ""
    else:
        # Default to nothing
        return ""

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


def log_all_tasks_stack_trace():
    stack_trace_data = StringIO()
    active_tasks = asyncio.all_tasks()
    active_tasks_no = len(active_tasks)
    for task in active_tasks:
        task.print_stack(file=stack_trace_data)
    logger.info(stack_trace_data.getvalue())
    logger.info(f"Total number of tasks: {active_tasks_no}")
