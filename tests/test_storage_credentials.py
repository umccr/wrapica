"""Tests for wrapica.storage_credentials functions."""

import pytest
from unittest.mock import patch

from tests.test_helpers import DUMMY_S3_BUCKET, DUMMY_S3_KEY_PREFIX


# Mock credentials data used across tests
MOCK_CREDENTIALS = [
    {
        "id": "cred-id-1",
        "name": "my-cred",
        "s3UriList": [
            {"bucketName": DUMMY_S3_BUCKET, "keyPrefix": DUMMY_S3_KEY_PREFIX}
        ],
    },
    {
        "id": "cred-id-2",
        "name": "other-cred",
        "s3UriList": [
            {"bucketName": "other-bucket", "keyPrefix": "other/prefix/"}
        ],
    },
]

PATCH_TARGET = (
    "wrapica.storage_credentials.functions.storage_credentials_functions"
    ".get_storage_credential_list"
)


class TestGetStorageCredentialIdFromS3Uri:
    """Tests for get_storage_credential_id_from_s3_uri."""

    def test_matching_bucket_and_prefix_returns_credential_id(self):
        """Requirement 13.1: matching bucket/prefix returns the credential ID."""
        s3_uri = f"s3://{DUMMY_S3_BUCKET}/{DUMMY_S3_KEY_PREFIX}path/to/file.txt"

        with patch(PATCH_TARGET, return_value=MOCK_CREDENTIALS):
            from wrapica.storage_credentials.functions.storage_credentials_functions import (
                get_storage_credential_id_from_s3_uri,
            )

            result = get_storage_credential_id_from_s3_uri(s3_uri)

        assert result == "cred-id-1"

    def test_non_matching_uri_returns_none(self):
        """Requirement 13.2: non-matching URI returns None."""
        s3_uri = "s3://unregistered-bucket/some/other/path/file.txt"

        with patch(PATCH_TARGET, return_value=MOCK_CREDENTIALS):
            from wrapica.storage_credentials.functions.storage_credentials_functions import (
                get_storage_credential_id_from_s3_uri,
            )

            result = get_storage_credential_id_from_s3_uri(s3_uri)

        assert result is None

    def test_matching_second_credential(self):
        """Verify the function can match a credential that's not the first in the list."""
        s3_uri = "s3://other-bucket/other/prefix/data.csv"

        with patch(PATCH_TARGET, return_value=MOCK_CREDENTIALS):
            from wrapica.storage_credentials.functions.storage_credentials_functions import (
                get_storage_credential_id_from_s3_uri,
            )

            result = get_storage_credential_id_from_s3_uri(s3_uri)

        assert result == "cred-id-2"


class TestGetRelativePathFromCredentialsPrefix:
    """Tests for get_relative_path_from_credentials_prefix."""

    def test_returns_relative_path_after_prefix(self):
        """Requirement 13.3: returns path segment after the credential's key prefix."""
        s3_uri = f"s3://{DUMMY_S3_BUCKET}/{DUMMY_S3_KEY_PREFIX}path/to/file.txt"

        with patch(PATCH_TARGET, return_value=MOCK_CREDENTIALS):
            from wrapica.storage_credentials.functions.storage_credentials_functions import (
                get_relative_path_from_credentials_prefix,
            )

            result = get_relative_path_from_credentials_prefix("cred-id-1", s3_uri)

        assert result == "path/to/file.txt"

    def test_preserves_trailing_slash(self):
        """Requirement 13.3: trailing slash in input URI is preserved in output."""
        s3_uri = f"s3://{DUMMY_S3_BUCKET}/{DUMMY_S3_KEY_PREFIX}path/to/folder/"

        with patch(PATCH_TARGET, return_value=MOCK_CREDENTIALS):
            from wrapica.storage_credentials.functions.storage_credentials_functions import (
                get_relative_path_from_credentials_prefix,
            )

            result = get_relative_path_from_credentials_prefix("cred-id-1", s3_uri)

        assert result == "path/to/folder/"
