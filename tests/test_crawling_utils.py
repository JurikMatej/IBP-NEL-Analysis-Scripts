import asyncio
import pytest
from unittest.mock import patch, MagicMock

from src import crawling_utils


pytest_plugins = ('pytest_asyncio',)


class TestCrawlingUtils:

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
    def test_filter_fragments_from_links_fragment_is_filtered(self, input_links, expected_links):
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
    async def test_get_unique_page_links_list_of_dicts_type_error_handled(self, page):
        invalid_input = [{}]
        page.eval_on_selector_all = MagicMock(return_value=asyncio.sleep(0.1, result=invalid_input))

        result = await crawling_utils._get_unique_page_links(page)

        assert result == []
