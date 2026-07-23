"""Tests for wrapica.data module (non-project data functions)."""

import pytest
from unittest.mock import MagicMock

from libica.openapi.v3 import ApiException
from libica.openapi.v3.api.data_api import DataApi

from wrapica.data import (
    get_data_obj_from_data_id,
    get_owning_project_id,
    convert_data_obj_to_icav2_uri,
    coerce_data_id_path_or_icav2_uri_to_data_obj,
)

from tests.test_helpers import (
    DUMMY_DATA_ID_FILE,
    DUMMY_DATA_ID_FOLDER,
    DUMMY_PROJECT_ID,
    DUMMY_REGION_ID,
)


class TestGetDataObjFromDataId:
    """Tests for get_data_obj_from_data_id."""

    def test_constructs_urn_and_returns_data_obj(self, mocker, configuration_fixture):
        """Verify URN is constructed correctly and data object is returned."""
        # Patch get_icav2_configuration where it's used (top-level import binding)
        mocker.patch(
            "wrapica.data.functions.data_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        # Mock get_default_region at its source (lazily imported inside function body)
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID
        mocker.patch(
            "wrapica.region.functions.region_functions.get_default_region",
            return_value=mock_region,
        )

        # Mock DataApi.get_data to return a data object
        mock_data = MagicMock()
        mock_data.id = DUMMY_DATA_ID_FILE
        mock_get_data = mocker.patch.object(
            DataApi, "get_data", return_value=mock_data
        )

        result = get_data_obj_from_data_id(DUMMY_DATA_ID_FILE)

        expected_urn = f"urn:ilmn:ica:region:{DUMMY_REGION_ID}:data:{DUMMY_DATA_ID_FILE}"
        mock_get_data.assert_called_once_with(data_urn=expected_urn)
        assert result is mock_data

    def test_uses_provided_region_id(self, mocker, configuration_fixture):
        """Verify that when region_id is provided, get_default_region is not called."""
        mocker.patch(
            "wrapica.data.functions.data_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mock_default_region = mocker.patch(
            "wrapica.region.functions.region_functions.get_default_region",
        )

        mock_data = MagicMock()
        mock_get_data = mocker.patch.object(
            DataApi, "get_data", return_value=mock_data
        )

        result = get_data_obj_from_data_id(DUMMY_DATA_ID_FILE, region_id=DUMMY_REGION_ID)

        mock_default_region.assert_not_called()
        expected_urn = f"urn:ilmn:ica:region:{DUMMY_REGION_ID}:data:{DUMMY_DATA_ID_FILE}"
        mock_get_data.assert_called_once_with(data_urn=expected_urn)
        assert result is mock_data

    def test_api_exception_propagated(self, mocker, configuration_fixture):
        """Verify ApiException propagates from DataApi.get_data."""
        mocker.patch(
            "wrapica.data.functions.data_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID
        mocker.patch(
            "wrapica.region.functions.region_functions.get_default_region",
            return_value=mock_region,
        )

        mocker.patch.object(
            DataApi, "get_data", side_effect=ApiException("Not found")
        )

        with pytest.raises(ApiException):
            get_data_obj_from_data_id(DUMMY_DATA_ID_FILE)


class TestGetOwningProjectId:
    """Tests for get_owning_project_id."""

    def test_returns_owning_project_id_as_string(self, mocker):
        """Verify the owning project ID is returned as a string from the data object."""
        mock_data = MagicMock()
        mock_data.details.owning_project_id = DUMMY_PROJECT_ID

        mocker.patch(
            "wrapica.data.functions.data_functions.get_data_obj_from_data_id",
            return_value=mock_data,
        )

        result = get_owning_project_id(DUMMY_DATA_ID_FILE)

        assert result == DUMMY_PROJECT_ID
        assert isinstance(result, str)


class TestConvertDataObjToIcav2Uri:
    """Tests for convert_data_obj_to_icav2_uri."""

    def test_uri_format(self):
        """Verify the returned URI matches icav2://{project_id}{path}."""
        mock_data = MagicMock()
        mock_data.details.owning_project_id = DUMMY_PROJECT_ID
        mock_data.details.path = "/path/to/file.txt"

        result = convert_data_obj_to_icav2_uri(mock_data)

        assert result == f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"

    def test_uri_format_folder(self):
        """Verify the returned URI works for folder paths ending with /."""
        mock_data = MagicMock()
        mock_data.details.owning_project_id = DUMMY_PROJECT_ID
        mock_data.details.path = "/path/to/folder/"

        result = convert_data_obj_to_icav2_uri(mock_data)

        assert result == f"icav2://{DUMMY_PROJECT_ID}/path/to/folder/"


class TestCoerceDataIdPathOrIcav2UriToDataObj:
    """Tests for coerce_data_id_path_or_icav2_uri_to_data_obj."""

    def test_data_id_format_file(self, mocker):
        """Verify data ID format (fil.*) delegates to get_data_obj_from_data_id."""
        mock_data = MagicMock()
        mock_get_data = mocker.patch(
            "wrapica.data.functions.data_functions.get_data_obj_from_data_id",
            return_value=mock_data,
        )

        result = coerce_data_id_path_or_icav2_uri_to_data_obj(DUMMY_DATA_ID_FILE)

        mock_get_data.assert_called_once_with(data_id=DUMMY_DATA_ID_FILE)
        assert result is mock_data

    def test_data_id_format_folder(self, mocker):
        """Verify data ID format (fol.*) delegates to get_data_obj_from_data_id."""
        mock_data = MagicMock()
        mock_get_data = mocker.patch(
            "wrapica.data.functions.data_functions.get_data_obj_from_data_id",
            return_value=mock_data,
        )

        result = coerce_data_id_path_or_icav2_uri_to_data_obj(DUMMY_DATA_ID_FOLDER)

        mock_get_data.assert_called_once_with(data_id=DUMMY_DATA_ID_FOLDER)
        assert result is mock_data

    def test_icav2_uri(self, mocker):
        """Verify icav2:// URI delegates to convert_icav2_uri_to_data_obj."""
        mock_data = MagicMock()
        mock_convert = mocker.patch(
            "wrapica.data.functions.data_functions.convert_icav2_uri_to_data_obj",
            return_value=mock_data,
        )

        uri = f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"
        result = coerce_data_id_path_or_icav2_uri_to_data_obj(uri)

        mock_convert.assert_called_once_with(
            data_uri=uri,
            create_data_if_not_found=False,
        )
        assert result is mock_data

    def test_icav2_uri_with_create_flag(self, mocker):
        """Verify create_data_if_not_found is passed through for icav2:// URI."""
        mock_data = MagicMock()
        mock_convert = mocker.patch(
            "wrapica.data.functions.data_functions.convert_icav2_uri_to_data_obj",
            return_value=mock_data,
        )

        uri = f"icav2://{DUMMY_PROJECT_ID}/path/to/file.txt"
        result = coerce_data_id_path_or_icav2_uri_to_data_obj(
            uri, create_data_if_not_found=True
        )

        mock_convert.assert_called_once_with(
            data_uri=uri,
            create_data_if_not_found=True,
        )
        assert result is mock_data

    def test_root_path_returns_none(self, mocker):
        """Verify '/' path returns None."""
        # Mock get_project_id at its source (lazily imported inside function body)
        mocker.patch(
            "wrapica.project.functions.project_functions.get_project_id",
            return_value=DUMMY_PROJECT_ID,
        )

        result = coerce_data_id_path_or_icav2_uri_to_data_obj("/")

        assert result is None
