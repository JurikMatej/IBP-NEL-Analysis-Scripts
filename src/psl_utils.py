import logging
import pathlib
import re
import sys
from typing import List
import publicsuffix2 as psl2


PATTERN_MULTIPLE_NEWLINES = r"\n+"
PATTERN_PSL_COMMENT = re.compile(r"^([\s]*|[/]{2}[\s]*.*)", flags=re.MULTILINE)


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


def get_sld_from_custom_psl(domain, custom_psl):
    """
    Get the SLD of a given domain. In terms of a PSL, SLD - Second Level Domain - represents eTLD+1

    TODO check if idna kwarg should be True or False - so far seems to be working

    :param domain: The domain to parse SLD from
    :param custom_psl: A parsed psl file's contents
    :return: SLD of the given domain (eTLD+1)
    """
    return psl2.get_sld(domain, custom_psl, wildcard=True, idna=True, strict=False)  # Using defaults here


def get_psl_for_specific_date(year: str, month: str, psl_dir: str, psl_files: List[pathlib.Path]) -> str:
    current_psl_path = pathlib.Path(f"{psl_dir}/psl_current.dat")
    historic_psl_path = pathlib.Path(f"{psl_dir}/psl_{year}_{month}.dat")

    if historic_psl_path in psl_files:
        return read_historic_psl_with_current_etlds(current_psl_path, historic_psl_path)
    else:
        logger.warning(f"No PSL available for {year}-{month}. Using the latest one: {current_psl_path.name}")
        return read_current_psl(current_psl_path)


def read_current_psl(current_psl_path: pathlib.Path) -> str:
    with open(current_psl_path, "r", encoding="utf-8") as current_psl_file:
        current_psl = parse_psl_content(current_psl_file.read())
        return current_psl


def read_historic_psl_with_current_etlds(current_psl_path: pathlib.Path, historic_psl_path: pathlib.Path) -> str:
    """
    Unify a historic PSL with the current one.

    Motivation to do so - the historic PSL might have some extra eTLDs, that were removed at some time.
    The current PSL might not have all the eTLDs necessary to analyze historic datasets possibly containing the
    historic eTLDs.
    Also, the historic PSL might NOT contain every eTLD from its time period as it is managed manually on GitHub.

    ...To analyze historic urls and parse their eTLD+1s, it is most safe to use merged eTLDs from historic PSL
    and the current one.

    The PSLs must be stored offline

    :param current_psl_path: Ideally, the latest available Public Suffix List
    :param historic_psl_path: Any historic Public Suffix List
    :return: Merged Public Suffix List with historic and currently registered eTLDs
    """
    with open(current_psl_path, "r", encoding="utf-8") as current_psl_file:
        current_psl = parse_psl_content(current_psl_file.read())
        unique_current_etlds = set(current_psl.split("\n"))

        with (open(historic_psl_path, "r", encoding="utf-8")) as historic_psl_file:
            historic_etlds = parse_psl_content(historic_psl_file.read())
            unique_historic_etlds = set(historic_etlds.split("\n"))

            merged_unique_etlds = unique_current_etlds.union(unique_historic_etlds)

    return "\n".join(merged_unique_etlds)


def parse_psl_content(file_content) -> str:
    no_comments_file_content = re.sub(PATTERN_PSL_COMMENT, "", file_content)
    etlds_text = re.sub(PATTERN_MULTIPLE_NEWLINES, "\n", no_comments_file_content)
    return etlds_text.strip()
