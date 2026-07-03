"""Shared test constants and helper functions for wrapica test suite."""

from unittest.mock import MagicMock


# Reusable test constants
DUMMY_PROJECT_ID = "aaaaaaaa-1111-4000-8000-aaaaaaaaaaaa"
DUMMY_DATA_ID_FILE = "fil.1234567890abcdef1234567890abcdef"
DUMMY_DATA_ID_FOLDER = "fol.abcdef1234567890abcdef1234567890"
DUMMY_REGION_ID = "bbbbbbbb-2222-4000-8000-bbbbbbbbbbbb"
DUMMY_PIPELINE_ID = "cccccccc-3333-4000-8000-cccccccccccc"
DUMMY_BUNDLE_ID = "dddddddd-4444-4000-8000-dddddddddddd"
DUMMY_USER_ID = "eeeeeeee-5555-4000-8000-eeeeeeeeeeee"
DUMMY_JOB_ID = "ffffffff-6666-4000-8000-ffffffffffff"
DUMMY_ANALYSIS_ID = "11111111-7777-4000-8000-111111111111"

DUMMY_S3_BUCKET = "pipeline-dev-cache-503977275616-ap-southeast-2"
DUMMY_S3_KEY_PREFIX = "ilmn-ica/project/aaaaaaaa-1111-4000-8000-aaaaaaaaaaaa/"
DUMMY_ICAV2_URI = f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"
DUMMY_S3_URI = f"s3://{DUMMY_S3_BUCKET}/{DUMMY_S3_KEY_PREFIX}path/to/file.txt"


def make_paged_response(items_per_page: list[list], use_page_token=True):
    """Helper to construct multi-page mock responses.

    Args:
        items_per_page: A list of lists, where each inner list represents the items
            returned on a single page.
        use_page_token: If True, sets next_page_token for pagination. If False,
            sets total_item_count for count-based pagination.

    Returns:
        A list of MagicMock response objects simulating paginated API responses.
    """
    responses = []
    for i, items in enumerate(items_per_page):
        resp = MagicMock()
        resp.items = items
        if use_page_token:
            resp.next_page_token = f"token_{i+1}" if i < len(items_per_page) - 1 else None
        else:
            resp.total_item_count = sum(len(p) for p in items_per_page)
        responses.append(resp)
    return responses
