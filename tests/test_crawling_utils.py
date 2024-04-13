import asyncio
import pytest
from unittest.mock import patch, MagicMock

from src import crawling_utils


pytest_plugins = ('pytest_asyncio',)


class TestCrawlingUtils:

    @pytest.mark.parametrize(
        'content_type, expected_result',
        (
            ('application', 'other'),
            ('text', 'text'),
            ('video', 'video'),
            ('audio', 'audio'),
            ('font', 'font'),
            ('image', 'image'),
            ('_', 'other'),
        )
    )
    def test_parse_content_type__only_category_provided(self, content_type, expected_result):
        result = crawling_utils.parse_content_type(content_type)
        assert result == expected_result

    @pytest.mark.parametrize(
        'content_type, expected_result',
        (
            ('application/xml', 'xml'),
            ('application/javascript', 'script'),
            ('application/_', 'other'),
            ('text/html', 'html'),
            ('text/css', 'css'),
            ('text/javascript', 'script'),
            ('text/_', 'text'),
            ('video/_', 'video'),
            ('audio/_', 'audio'),
            ('font/_', 'font'),
            ('image/_', 'image'),
            ('_/_', 'other'),
        )
    )
    def test_content_type_to_httparchive_type(self, content_type, expected_result):
        category, specific = content_type.split('/')
        result = crawling_utils._content_type_to_httparchive_type(category, specific)

        assert result == expected_result

    @pytest.mark.parametrize(
        "input_links, expected_links", (
            (
                ["https://example.com/"],
                ["https://example.com/"]
            ),
            (
                ["https://example.com/#FRAGMENT"],
                ["https://example.com/"],
            ),
            (
                ["https://example.com/sub/page/1/#FRAGMENT"],
                ["https://example.com/sub/page/1/"],
            ),
            (
                ["https://example.com/#"],
                ["https://example.com/"],
            ),
            (
                ["https://example.com/###"],
                ["https://example.com/"],
            ),
            (
                ["https://example.com/#f1#f2#f3#invalid"],
                ["https://example.com/"],
            )
        )
    )
    def test_filter_fragments_from_links__fragment_is_filtered(self, input_links, expected_links):
        result = crawling_utils._filter_fragments_from_links(input_links)
        assert result[0] == expected_links[0]

    @pytest.mark.parametrize(
        "input_links, expected_links", (
            (
                ["https://example.com/?a=b&c=d"],
                ["https://example.com/?a=b&c=d"]
            ),
            (
                ["https://example.com/#FRAGMENT?a=b&c=d"],
                ["https://example.com/?a=b&c=d"],
            ),
            (
                ["https://example.com/sub/page/1/#FRAGMENT?a=b&c=d"],
                ["https://example.com/sub/page/1/?a=b&c=d"],
            ),
            (
                ["https://example.com/#?a=b&c=d"],
                ["https://example.com/?a=b&c=d"],
            )
        )
    )
    def test_filter_fragments_from_links__qs_is_not_filtered(self, input_links, expected_links):
        result = crawling_utils._filter_fragments_from_links(input_links)
        assert result[0] == expected_links[0]

    @patch("playwright.async_api.Page")
    @pytest.mark.asyncio
    async def test_get_unique_page_links__list_of_dicts_type_error_handled(self, page):
        invalid_input = [{}]
        page.eval_on_selector_all = MagicMock(return_value=asyncio.sleep(0.1, result=invalid_input))

        result = await crawling_utils._get_unique_page_links(page)

        assert result == []

    @pytest.mark.parametrize(
        "input_link, expected",
        [
            ("https://example.com/", ""),
            ("https://example.com", ""),
            ("example.com", ""),
        ]
    )
    def test_absolute_to_relative_link__base_domain_returns_empty_link(self, input_link, expected):
        assert crawling_utils._absolute_to_relative_link(input_link, "example.com") == expected

    @pytest.mark.parametrize(
        "input_link, expected",
        [
            ("https://example.com/sub",             "sub"),
            ("https://example.com/sub/page",        "sub/page"),
            ("https://example.com/sub/page?qs=qs",  "sub/page?qs=qs"),
            ("example.com/sub",                     "sub"),
            ("example.com/sub/page",                "sub/page"),
            ("example.com/sub/page?qs=qs",          "sub/page?qs=qs"),
        ]
    )
    def test_absolute_to_relative_link__sub_paths_without_leading_slash(self, input_link, expected):
        assert crawling_utils._absolute_to_relative_link(input_link, "example.com") == expected
