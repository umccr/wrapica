"""Tests for wrapica.pipelines module."""
import zipfile
from unittest.mock import MagicMock

import pytest
from libica.openapi.v3 import ApiException
from libica.openapi.v3.api.pipeline_api import PipelineApi

from tests.test_helpers import DUMMY_PIPELINE_ID

from wrapica.pipelines.functions.pipelines_functions import (
    get_pipeline_obj_from_pipeline_id,
    list_all_pipelines,
    coerce_pipeline_id_or_code_to_pipeline_obj,
    download_pipeline_to_zip,
    list_pipeline_files,
)

MODULE_PATH = "wrapica.pipelines.functions.pipelines_functions"


class TestGetPipelineObjFromPipelineId:
    """Tests for get_pipeline_obj_from_pipeline_id."""

    def test_returns_pipeline_object(self, mocker, configuration_fixture):
        """Verify the function returns the mocked PipelineV4 instance."""
        mock_pipeline = MagicMock()
        mock_pipeline.id = DUMMY_PIPELINE_ID
        mock_pipeline.code = "test-pipeline"

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get_pipeline = mocker.patch.object(
            PipelineApi, "get_pipeline",
            return_value=mock_pipeline,
        )

        result = get_pipeline_obj_from_pipeline_id(DUMMY_PIPELINE_ID)

        assert result is mock_pipeline
        mock_get_pipeline.assert_called_once_with(pipeline_id=DUMMY_PIPELINE_ID)

    def test_api_exception_propagated(self, mocker, configuration_fixture):
        """Verify ApiException propagates from PipelineApi.get_pipeline."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            PipelineApi, "get_pipeline",
            side_effect=ApiException("Not found"),
        )

        with pytest.raises(ApiException):
            get_pipeline_obj_from_pipeline_id(DUMMY_PIPELINE_ID)


class TestListAllPipelines:
    """Tests for list_all_pipelines."""

    def test_returns_pipeline_items(self, mocker, configuration_fixture):
        """Verify the function returns the items list from the API response."""
        mock_pipeline_1 = MagicMock()
        mock_pipeline_1.id = "aaaa1111-1111-4000-8000-aaaaaaaaaaaa"
        mock_pipeline_1.code = "pipeline-one"

        mock_pipeline_2 = MagicMock()
        mock_pipeline_2.id = "bbbb2222-2222-4000-8000-bbbbbbbbbbbb"
        mock_pipeline_2.code = "pipeline-two"

        mock_response = MagicMock()
        mock_response.items = [mock_pipeline_1, mock_pipeline_2]

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get_pipelines = mocker.patch.object(
            PipelineApi, "get_pipelines",
            return_value=mock_response,
        )

        result = list_all_pipelines()

        assert result == [mock_pipeline_1, mock_pipeline_2]
        assert len(result) == 2
        mock_get_pipelines.assert_called_once()


class TestCoercePipelineIdOrCodeToPipelineObj:
    """Tests for coerce_pipeline_id_or_code_to_pipeline_obj."""

    def test_uuid_delegates_to_get_by_id(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_pipeline_obj_from_pipeline_id."""
        mock_pipeline = MagicMock()
        mock_pipeline.id = DUMMY_PIPELINE_ID

        mock_get_by_id = mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        result = coerce_pipeline_id_or_code_to_pipeline_obj(DUMMY_PIPELINE_ID)

        assert result is mock_pipeline
        mock_get_by_id.assert_called_once_with(DUMMY_PIPELINE_ID)

    def test_non_uuid_delegates_to_get_by_code(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_pipeline_obj_from_pipeline_code."""
        mock_pipeline = MagicMock()
        mock_pipeline.code = "my-pipeline-code"

        mock_get_by_code = mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.get_pipeline_obj_from_pipeline_code",
            return_value=mock_pipeline,
        )

        result = coerce_pipeline_id_or_code_to_pipeline_obj("my-pipeline-code")

        assert result is mock_pipeline
        mock_get_by_code.assert_called_once_with("my-pipeline-code")


class TestListPipelineFiles:
    """Tests for list_pipeline_files."""

    def test_returns_pipeline_file_items(self, mocker, configuration_fixture):
        """Verify the function returns the items list from the API response."""
        mock_file_1 = MagicMock()
        mock_file_1.id = "file-id-1"
        mock_file_1.name = "workflow.cwl"

        mock_file_2 = MagicMock()
        mock_file_2.id = "file-id-2"
        mock_file_2.name = "tools/tool.cwl"

        mock_response = MagicMock()
        mock_response.items = [mock_file_1, mock_file_2]

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get_pipeline_files = mocker.patch.object(
            PipelineApi, "get_pipeline_files",
            return_value=mock_response,
        )

        result = list_pipeline_files(DUMMY_PIPELINE_ID)

        assert result == [mock_file_1, mock_file_2]
        assert len(result) == 2
        mock_get_pipeline_files.assert_called_once_with(pipeline_id=DUMMY_PIPELINE_ID)


class TestDownloadPipelineToZip:
    """Tests for download_pipeline_to_zip."""

    def test_creates_zip_with_expected_files(self, mocker, configuration_fixture, tmp_path):
        """Verify a zip archive is created at the specified path containing expected files."""
        # Mock get_pipeline_obj_from_pipeline_id
        mock_pipeline = MagicMock()
        mock_pipeline.code = "test-pipeline"
        mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        # Mock list_pipeline_files
        mock_file_1 = MagicMock()
        mock_file_1.id = "file-id-1"
        mock_file_1.name = "workflow.cwl"

        mock_file_2 = MagicMock()
        mock_file_2.id = "file-id-2"
        mock_file_2.name = "tools/tool.cwl"

        mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.list_pipeline_files",
            return_value=[mock_file_1, mock_file_2],
        )

        # Mock download_pipeline_file to write dummy content
        def fake_download(pipeline_id, file_id, file_path):
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(f"content of {file_id}")
            return None

        mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.download_pipeline_file",
            side_effect=fake_download,
        )

        zip_path = tmp_path / "output.zip"
        download_pipeline_to_zip(DUMMY_PIPELINE_ID, zip_path)

        # Verify the zip file was created
        assert zip_path.exists()

        # Verify contents of the zip
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            assert "test-pipeline/workflow.cwl" in names
            assert "test-pipeline/tools/tool.cwl" in names
