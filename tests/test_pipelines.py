"""Tests for wrapica.pipelines module."""
import zipfile
from unittest.mock import MagicMock

import pytest
from libica.openapi.v3 import ApiException
from libica.openapi.v3.api.pipeline_api import PipelineApi

from tests.test_helpers import DUMMY_PIPELINE_ID

from wrapica.pipelines.functions.pipelines_functions import (
    get_pipeline_obj_from_pipeline_id,
    get_pipeline_obj_from_pipeline_name,
    list_all_pipelines,
    coerce_pipeline_id_or_name_to_pipeline_obj,
    coerce_pipeline_id_or_name_to_pipeline_id,
    coerce_pipeline_id_or_code_to_pipeline_obj,
    coerce_pipeline_id_or_code_to_pipeline_id,
    download_pipeline_to_zip,
    list_pipeline_files,
)

MODULE_PATH = "wrapica.pipelines.functions.pipelines_functions"


def _make_v4(name=None, version=None, latest=None, group_id=None, pipeline_id=None):
    """Build a mock PipelineV4-like object for name/version resolution tests."""
    mock = MagicMock()
    mock.name = name
    mock.version = version
    mock.latest_version_in_group = latest
    mock.id = pipeline_id
    if group_id is None:
        mock.pipeline_group = None
    else:
        mock.pipeline_group = MagicMock()
        mock.pipeline_group.id = group_id
    return mock


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


class TestGetPipelineObjFromPipelineName:
    """Tests for get_pipeline_obj_from_pipeline_name."""

    def _patch_tenant(self, mocker, v4_objs):
        """Patch list_all_pipelines (v3 ids) and get_pipeline_obj_from_pipeline_id (v4)."""
        v3_objs = [MagicMock(id=f"id-{idx}") for idx, _ in enumerate(v4_objs)]
        mock_list = mocker.patch(
            f"{MODULE_PATH}.list_all_pipelines",
            return_value=v3_objs,
        )
        mock_get_by_id = mocker.patch(
            f"{MODULE_PATH}.get_pipeline_obj_from_pipeline_id",
            side_effect=list(v4_objs),
        )
        return mock_list, mock_get_by_id

    def test_empty_name_raises_without_api_call(self, mocker, configuration_fixture):
        """Verify an empty name raises ValueError before any API call."""
        mock_list = mocker.patch(f"{MODULE_PATH}.list_all_pipelines")

        with pytest.raises(ValueError):
            get_pipeline_obj_from_pipeline_name("")

        mock_list.assert_not_called()

    def test_exact_version_match(self, mocker, configuration_fixture):
        """Verify an explicit version returns the exact match."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0")
        obj_2 = _make_v4(name="my-pipeline", version="2.0.0")
        self._patch_tenant(mocker, [obj_1, obj_2])

        result = get_pipeline_obj_from_pipeline_name("my-pipeline", "2.0.0")

        assert result is obj_2

    def test_single_match_returned_when_flag_false(self, mocker, configuration_fixture):
        """Verify a single name match is returned regardless of its latest flag."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0", latest=False)
        obj_other = _make_v4(name="other", version="1.0.0")
        self._patch_tenant(mocker, [obj_1, obj_other])

        result = get_pipeline_obj_from_pipeline_name("my-pipeline")

        assert result is obj_1

    def test_latest_default_returns_flagged(self, mocker, configuration_fixture):
        """Verify the latest-in-group flagged version is returned when version omitted."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0", latest=False)
        obj_2 = _make_v4(name="my-pipeline", version="2.0.0", latest=True)
        self._patch_tenant(mocker, [obj_1, obj_2])

        result = get_pipeline_obj_from_pipeline_name("my-pipeline")

        assert result is obj_2

    def test_not_found_raises(self, mocker, configuration_fixture):
        """Verify ValueError is raised when the name is not found."""
        obj_other = _make_v4(name="other", version="1.0.0")
        self._patch_tenant(mocker, [obj_other])

        with pytest.raises(ValueError):
            get_pipeline_obj_from_pipeline_name("my-pipeline")

    def test_ambiguous_zero_flagged_raises(self, mocker, configuration_fixture):
        """Verify ValueError when multiple matches and none flagged latest."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0", latest=None)
        obj_2 = _make_v4(name="my-pipeline", version="2.0.0", latest=False)
        self._patch_tenant(mocker, [obj_1, obj_2])

        with pytest.raises(ValueError):
            get_pipeline_obj_from_pipeline_name("my-pipeline")

    def test_ambiguous_multi_flagged_lists_group_ids(self, mocker, configuration_fixture):
        """Verify ValueError lists the distinct group ids when >1 flagged latest."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0", latest=True, group_id="group-a")
        obj_2 = _make_v4(name="my-pipeline", version="2.0.0", latest=True, group_id="group-b")
        self._patch_tenant(mocker, [obj_1, obj_2])

        with pytest.raises(ValueError) as exc_info:
            get_pipeline_obj_from_pipeline_name("my-pipeline")

        assert "group-a" in str(exc_info.value)
        assert "group-b" in str(exc_info.value)

    def test_fetches_all_ids_no_short_circuit(self, mocker, configuration_fixture):
        """Verify every tenant pipeline id is fetched as a v4 object (no short-circuit)."""
        obj_1 = _make_v4(name="my-pipeline", version="1.0.0", latest=True)
        obj_2 = _make_v4(name="my-pipeline", version="2.0.0", latest=False)
        obj_3 = _make_v4(name="other", version="1.0.0")
        _, mock_get_by_id = self._patch_tenant(mocker, [obj_1, obj_2, obj_3])

        get_pipeline_obj_from_pipeline_name("my-pipeline")

        assert mock_get_by_id.call_count == 3


class TestCoercePipelineIdOrNameToPipelineObj:
    """Tests for coerce_pipeline_id_or_name_to_pipeline_obj / _id."""

    def test_obj_uuid_delegates_to_get_by_id(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_pipeline_obj_from_pipeline_id."""
        mock_pipeline = MagicMock()
        mock_get_by_id = mocker.patch(
            f"{MODULE_PATH}.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        result = coerce_pipeline_id_or_name_to_pipeline_obj(DUMMY_PIPELINE_ID)

        assert result is mock_pipeline
        mock_get_by_id.assert_called_once_with(DUMMY_PIPELINE_ID)

    def test_obj_non_uuid_delegates_to_get_by_name(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_pipeline_obj_from_pipeline_name."""
        mock_pipeline = MagicMock()
        mock_get_by_name = mocker.patch(
            f"{MODULE_PATH}.get_pipeline_obj_from_pipeline_name",
            return_value=mock_pipeline,
        )

        result = coerce_pipeline_id_or_name_to_pipeline_obj("my-pipeline", "1.0.0")

        assert result is mock_pipeline
        mock_get_by_name.assert_called_once_with("my-pipeline", "1.0.0")

    def test_id_uuid_returns_input(self, mocker, configuration_fixture):
        """Verify UUID input is returned as-is."""
        result = coerce_pipeline_id_or_name_to_pipeline_id(DUMMY_PIPELINE_ID)
        assert result == DUMMY_PIPELINE_ID

    def test_id_non_uuid_delegates_to_get_by_name(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_pipeline_obj_from_pipeline_name.id."""
        mock_pipeline = MagicMock()
        mock_pipeline.id = DUMMY_PIPELINE_ID
        mock_get_by_name = mocker.patch(
            f"{MODULE_PATH}.get_pipeline_obj_from_pipeline_name",
            return_value=mock_pipeline,
        )

        result = coerce_pipeline_id_or_name_to_pipeline_id("my-pipeline")

        assert result == DUMMY_PIPELINE_ID
        mock_get_by_name.assert_called_once_with("my-pipeline", None)


class TestCoercePipelineIdOrCodeToPipelineObj:
    """Tests for the deprecated coerce_pipeline_id_or_code_* aliases."""

    def test_uuid_delegates_to_get_by_id(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_pipeline_obj_from_pipeline_id."""
        mock_pipeline = MagicMock()
        mock_pipeline.id = DUMMY_PIPELINE_ID

        mock_get_by_id = mocker.patch(
            f"{MODULE_PATH}.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        with pytest.warns(DeprecationWarning):
            result = coerce_pipeline_id_or_code_to_pipeline_obj(DUMMY_PIPELINE_ID)

        assert result is mock_pipeline
        mock_get_by_id.assert_called_once_with(DUMMY_PIPELINE_ID)

    def test_obj_non_uuid_delegates_to_name_resolver(self, mocker, configuration_fixture):
        """Verify the deprecated obj alias delegates to the NAME coerce resolver."""
        mock_pipeline = MagicMock()
        mock_delegate = mocker.patch(
            f"{MODULE_PATH}.coerce_pipeline_id_or_name_to_pipeline_obj",
            return_value=mock_pipeline,
        )

        with pytest.warns(DeprecationWarning):
            result = coerce_pipeline_id_or_code_to_pipeline_obj("my-pipeline-code")

        assert result is mock_pipeline
        mock_delegate.assert_called_once_with("my-pipeline-code")

    def test_id_non_uuid_delegates_to_name_resolver(self, mocker, configuration_fixture):
        """Verify the deprecated id alias delegates to the NAME coerce resolver."""
        mock_delegate = mocker.patch(
            f"{MODULE_PATH}.coerce_pipeline_id_or_name_to_pipeline_id",
            return_value=DUMMY_PIPELINE_ID,
        )

        with pytest.warns(DeprecationWarning):
            result = coerce_pipeline_id_or_code_to_pipeline_id("my-pipeline-code")

        assert result == DUMMY_PIPELINE_ID
        mock_delegate.assert_called_once_with("my-pipeline-code")


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
        mock_pipeline.name = "test-pipeline"
        mock_pipeline.version = None
        mock_pipeline.id = DUMMY_PIPELINE_ID
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
