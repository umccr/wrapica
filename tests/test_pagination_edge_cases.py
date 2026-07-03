"""Pagination edge case tests for wrapica.

Tests cover:
- Single-page responses (no next_page_token)
- Empty first page (no items, no token)
- Error during subsequent page raises exception without partial results

Requirements: 16.1, 16.2, 16.3, 16.4
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from libica.openapi.v3 import ApiException

from tests.test_helpers import DUMMY_PROJECT_ID, make_paged_response


class TestPaginationSinglePage:
    """Test single-page response returns items with exactly 1 API call."""

    @patch(
        "wrapica.project_data.functions.project_data_functions.get_icav2_configuration",
        return_value=MagicMock(),
    )
    @patch(
        "libica.openapi.v3.api.project_data_api.ProjectDataApi.get_project_data_list"
    )
    def test_single_page_no_next_page_token(self, mock_get_list, mock_config):
        """When API returns items with next_page_token=None, return items with 1 call."""
        from wrapica.project_data.functions.project_data_functions import (
            list_project_data_non_recursively,
        )

        # Set up a single-page response with items and no next_page_token
        item1 = MagicMock(name="item1")
        item2 = MagicMock(name="item2")
        response = MagicMock()
        response.items = [item1, item2]
        response.next_page_token = None

        mock_get_list.return_value = response

        result = list_project_data_non_recursively(
            project_id=DUMMY_PROJECT_ID,
            parent_folder_path=Path("/test/folder/"),
        )

        # Verify returned list matches expected items
        assert result == [item1, item2]
        # Verify the API was called exactly once
        assert mock_get_list.call_count == 1


class TestPaginationEmptyFirstPage:
    """Test empty first page returns empty list."""

    @patch(
        "wrapica.project_data.functions.project_data_functions.get_icav2_configuration",
        return_value=MagicMock(),
    )
    @patch(
        "libica.openapi.v3.api.project_data_api.ProjectDataApi.get_project_data_list"
    )
    def test_empty_first_page_no_token(self, mock_get_list, mock_config):
        """When API returns no items and empty next_page_token, return empty list."""
        from wrapica.project_data.functions.project_data_functions import (
            list_project_data_non_recursively,
        )

        response = MagicMock()
        response.items = []
        response.next_page_token = ""

        mock_get_list.return_value = response

        result = list_project_data_non_recursively(
            project_id=DUMMY_PROJECT_ID,
            parent_folder_path=Path("/empty/folder/"),
        )

        assert result == []
        assert mock_get_list.call_count == 1


class TestPaginationErrorDuringSubsequentPage:
    """Test error during subsequent page raises exception without partial results."""

    @patch(
        "wrapica.project_data.functions.project_data_functions.get_icav2_configuration",
        return_value=MagicMock(),
    )
    @patch(
        "libica.openapi.v3.api.project_data_api.ProjectDataApi.get_project_data_list"
    )
    def test_error_on_second_page_raises_without_partial_results(
        self, mock_get_list, mock_config
    ):
        """When second API call raises ApiException, exception propagates (no partial data)."""
        from wrapica.project_data.functions.project_data_functions import (
            list_project_data_non_recursively,
        )

        # First page succeeds with items and a next_page_token
        first_response = MagicMock()
        first_response.items = [MagicMock(name="item1"), MagicMock(name="item2")]
        first_response.next_page_token = "page2_token"

        # Second call raises ApiException
        mock_get_list.side_effect = [
            first_response,
            ApiException(status=500, reason="Internal Server Error"),
        ]

        # The function wraps ApiException in a ValueError
        with pytest.raises(ValueError, match="Exception when calling"):
            list_project_data_non_recursively(
                project_id=DUMMY_PROJECT_ID,
                parent_folder_path=Path("/data/folder/"),
            )

        # Verify both calls were made (first succeeded, second raised)
        assert mock_get_list.call_count == 2
