"""Tests for wrapica.storage_configuration module.

Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 18.6
"""
import pytest
from unittest.mock import patch

from tests.test_helpers import (
    DUMMY_PROJECT_ID,
    DUMMY_S3_BUCKET,
    DUMMY_S3_KEY_PREFIX,
    DUMMY_S3_URI,
    DUMMY_ICAV2_URI,
)

# Module path prefix for mocking
_MOD = "wrapica.storage_configuration.functions.storage_configuration_functions"

# Test data
MOCK_STORAGE_CONFIGS = [
    {
        "id": "sc-1",
        "bucketName": DUMMY_S3_BUCKET,
        "keyPrefix": DUMMY_S3_KEY_PREFIX,
        "storageCredentialId": "cred-1",
    }
]

MOCK_PROJECT_MAPPINGS = [
    {
        "id": DUMMY_PROJECT_ID,
        "name": "test-project",
        "storageConfigurationId": "sc-1",
        "prefix": "",
    }
]


class TestGetStorageConfigurationList:
    """Tests for get_storage_configuration_list."""

    def test_returns_mocked_api_list(self, mocker):
        """Verify get_storage_configuration_list returns the list from the API."""
        # Reset the global so it triggers the API path
        mocker.patch(f"{_MOD}.STORAGE_CONFIGURATION_OBJECT_LIST", None)
        mocker.patch(
            f"{_MOD}._get_storage_configuration_env_list",
            return_value=None,
        )
        mocker.patch(
            f"{_MOD}._get_storage_configuration_api_list",
            return_value=MOCK_STORAGE_CONFIGS,
        )

        from wrapica.storage_configuration import get_storage_configuration_list

        result = get_storage_configuration_list()

        assert result == MOCK_STORAGE_CONFIGS
        assert result[0]["id"] == "sc-1"
        assert result[0]["bucketName"] == DUMMY_S3_BUCKET
        assert result[0]["keyPrefix"] == DUMMY_S3_KEY_PREFIX
        assert result[0]["storageCredentialId"] == "cred-1"


class TestConvertS3UriToIcav2Uri:
    """Tests for convert_s3_uri_to_icav2_uri."""

    def test_matching_prefix_converts_correctly(self, mocker):
        """Verify S3 URI with matching prefix resolves to correct icav2:// URI."""
        mocker.patch(
            f"{_MOD}.get_project_to_storage_configuration_mapping_list",
            return_value=MOCK_PROJECT_MAPPINGS,
        )
        mocker.patch(
            f"{_MOD}.get_storage_configuration_list",
            return_value=MOCK_STORAGE_CONFIGS,
        )

        from wrapica.storage_configuration import convert_s3_uri_to_icav2_uri

        result = convert_s3_uri_to_icav2_uri(DUMMY_S3_URI)

        # Should produce icav2://<project_id>/path/to/file.txt
        assert result.startswith("icav2://")
        assert DUMMY_PROJECT_ID in result
        assert result == f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"

    def test_non_matching_prefix_raises_value_error(self, mocker):
        """Verify ValueError is raised when no project matches the S3 URI prefix."""
        mocker.patch(
            f"{_MOD}.get_project_to_storage_configuration_mapping_list",
            return_value=MOCK_PROJECT_MAPPINGS,
        )
        mocker.patch(
            f"{_MOD}.get_storage_configuration_list",
            return_value=MOCK_STORAGE_CONFIGS,
        )

        from wrapica.storage_configuration import convert_s3_uri_to_icav2_uri

        with pytest.raises(ValueError):
            convert_s3_uri_to_icav2_uri("s3://unknown-bucket/unknown-prefix/file.txt")


class TestConvertIcav2UriToS3Uri:
    """Tests for convert_icav2_uri_to_s3_uri."""

    def test_valid_icav2_uri_converts_correctly(self, mocker):
        """Verify icav2:// URI resolves to correct S3 URI."""
        # The s3 key prefix returned by get_s3_key_prefix_by_project_id
        s3_prefix = f"s3://{DUMMY_S3_BUCKET}/{DUMMY_S3_KEY_PREFIX}"

        # coerce_project_id_or_name_to_project_id is imported locally from wrapica.project
        mocker.patch(
            "wrapica.project.coerce_project_id_or_name_to_project_id",
            return_value=DUMMY_PROJECT_ID,
        )
        mocker.patch(
            f"{_MOD}.get_s3_key_prefix_by_project_id",
            return_value=s3_prefix,
        )

        from wrapica.storage_configuration import convert_icav2_uri_to_s3_uri

        result = convert_icav2_uri_to_s3_uri(DUMMY_ICAV2_URI)

        # DUMMY_ICAV2_URI = icav2://<project_id>/path/to/file.txt
        # Expected: s3://<bucket>/<key_prefix>path/to/file.txt
        assert result.startswith("s3://")
        assert DUMMY_S3_BUCKET in result
        assert result == DUMMY_S3_URI

    def test_unconfigured_project_raises_value_error(self, mocker):
        """Verify ValueError when project has no S3 key prefix configured."""
        mocker.patch(
            "wrapica.project.coerce_project_id_or_name_to_project_id",
            return_value=DUMMY_PROJECT_ID,
        )
        mocker.patch(
            f"{_MOD}.get_s3_key_prefix_by_project_id",
            return_value=None,
        )

        from wrapica.storage_configuration import convert_icav2_uri_to_s3_uri

        with pytest.raises(ValueError):
            convert_icav2_uri_to_s3_uri(DUMMY_ICAV2_URI)


class TestGetProjectIdByS3KeyPrefix:
    """Tests for get_project_id_by_s3_key_prefix."""

    def test_matching_prefix_returns_project_id(self, mocker):
        """Verify correct project ID is returned for a matching S3 prefix."""
        mocker.patch(
            f"{_MOD}.get_project_to_storage_configuration_mapping_list",
            return_value=MOCK_PROJECT_MAPPINGS,
        )
        mocker.patch(
            f"{_MOD}.get_storage_configuration_list",
            return_value=MOCK_STORAGE_CONFIGS,
        )

        from wrapica.storage_configuration import get_project_id_by_s3_key_prefix

        result = get_project_id_by_s3_key_prefix(DUMMY_S3_URI)

        assert result == DUMMY_PROJECT_ID


class TestUnpackS3Uri:
    """Tests for unpack_s3_uri."""

    def test_returns_project_id_and_data_path(self, mocker):
        """Verify unpack_s3_uri returns (project_id, data_path) tuple."""
        mocker.patch(
            f"{_MOD}.get_project_to_storage_configuration_mapping_list",
            return_value=MOCK_PROJECT_MAPPINGS,
        )
        mocker.patch(
            f"{_MOD}.get_storage_configuration_list",
            return_value=MOCK_STORAGE_CONFIGS,
        )

        from wrapica.storage_configuration import unpack_s3_uri

        project_id, data_path = unpack_s3_uri(DUMMY_S3_URI)

        assert project_id == DUMMY_PROJECT_ID
        assert data_path == "/path/to/file.txt"
