"""Tests for wrapica.project_pipelines module."""
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from libica.openapi.v3 import ApiException
from libica.openapi.v3.api.project_pipeline_api import ProjectPipelineApi
from libica.openapi.v3.api.project_analysis_api import ProjectAnalysisApi

from tests.test_helpers import (
    DUMMY_PROJECT_ID,
    DUMMY_PIPELINE_ID,
    DUMMY_ANALYSIS_ID,
    make_paged_response,
)

MODULE_PATH = "wrapica.project_pipelines.functions.project_pipelines_functions"


class TestGetProjectPipelineObj:
    """Tests for get_project_pipeline_obj."""

    def test_returns_pipeline_object(self, mocker, configuration_fixture):
        """Verify the function returns the mocked ProjectPipelineV4 instance."""
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline.id = DUMMY_PIPELINE_ID
        mock_pipeline.pipeline.code = "test-pipeline"

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get = mocker.patch.object(
            ProjectPipelineApi, "get_project_pipeline",
            return_value=mock_pipeline,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            get_project_pipeline_obj,
        )

        result = get_project_pipeline_obj(DUMMY_PROJECT_ID, DUMMY_PIPELINE_ID)

        assert result is mock_pipeline
        mock_get.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
        )

    def test_api_exception_raises_value_error(self, mocker, configuration_fixture):
        """Verify ApiException from the API is wrapped in ValueError."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectPipelineApi, "get_project_pipeline",
            side_effect=ApiException("Not found"),
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            get_project_pipeline_obj,
        )

        with pytest.raises(ValueError):
            get_project_pipeline_obj(DUMMY_PROJECT_ID, DUMMY_PIPELINE_ID)


class TestGetProjectPipelineObjFromPipelineCode:
    """Tests for get_project_pipeline_obj_from_pipeline_code."""

    def test_returns_matching_pipeline(self, mocker, configuration_fixture):
        """Verify the function returns the pipeline with matching code."""
        mock_pipeline_1 = MagicMock()
        mock_pipeline_1.pipeline.code = "other-pipeline"

        mock_pipeline_2 = MagicMock()
        mock_pipeline_2.pipeline.code = "target-pipeline"

        mocker.patch(
            f"{MODULE_PATH}.list_project_pipelines",
            return_value=[mock_pipeline_1, mock_pipeline_2],
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            get_project_pipeline_obj_from_pipeline_code,
        )

        result = get_project_pipeline_obj_from_pipeline_code(
            DUMMY_PROJECT_ID, "target-pipeline"
        )

        assert result is mock_pipeline_2

    def test_raises_value_error_when_not_found(self, mocker, configuration_fixture):
        """Verify ValueError is raised when pipeline code is not found."""
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline.code = "other-pipeline"

        mocker.patch(
            f"{MODULE_PATH}.list_project_pipelines",
            return_value=[mock_pipeline],
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            get_project_pipeline_obj_from_pipeline_code,
        )

        with pytest.raises(ValueError):
            get_project_pipeline_obj_from_pipeline_code(
                DUMMY_PROJECT_ID, "nonexistent-pipeline"
            )


class TestLaunchCwlWorkflow:
    """Tests for launch_cwl_workflow."""

    def test_returns_analysis_object(self, mocker, configuration_fixture):
        """Verify the function returns the analysis response from the API."""
        mock_analysis = MagicMock()
        mock_analysis.id = DUMMY_ANALYSIS_ID
        mock_analysis.status = "REQUESTED"

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_create = mocker.patch.object(
            ProjectAnalysisApi, "create_cwl_analysis_with_json_input",
            return_value=mock_analysis,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_cwl_workflow,
        )

        cwl_analysis_input = MagicMock()
        result = launch_cwl_workflow(DUMMY_PROJECT_ID, cwl_analysis_input)

        assert result is mock_analysis
        mock_create.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            create_cwl_with_json_input_analysis=cwl_analysis_input,
        )

    def test_passes_idempotency_key(self, mocker, configuration_fixture):
        """Verify idempotency_key is forwarded to the API call."""
        mock_analysis = MagicMock()

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_create = mocker.patch.object(
            ProjectAnalysisApi, "create_cwl_analysis_with_json_input",
            return_value=mock_analysis,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_cwl_workflow,
        )

        cwl_analysis_input = MagicMock()
        launch_cwl_workflow(DUMMY_PROJECT_ID, cwl_analysis_input, idempotency_key="key-123")

        mock_create.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            create_cwl_with_json_input_analysis=cwl_analysis_input,
            idempotency_key="key-123",
        )

    def test_api_exception_propagated(self, mocker, configuration_fixture):
        """Verify ApiException is re-raised from the API call."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectAnalysisApi, "create_cwl_analysis_with_json_input",
            side_effect=ApiException("Launch failed"),
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_cwl_workflow,
        )

        with pytest.raises(ApiException):
            launch_cwl_workflow(DUMMY_PROJECT_ID, MagicMock())


class TestLaunchNextflowWorkflow:
    """Tests for launch_nextflow_workflow."""

    def test_returns_analysis_object(self, mocker, configuration_fixture):
        """Verify the function returns the analysis response from the API."""
        mock_analysis = MagicMock()
        mock_analysis.id = DUMMY_ANALYSIS_ID
        mock_analysis.status = "REQUESTED"

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_create = mocker.patch.object(
            ProjectAnalysisApi, "create_nextflow_analysis_with_custom_input",
            return_value=mock_analysis,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_nextflow_workflow,
        )

        nf_analysis_input = MagicMock()
        result = launch_nextflow_workflow(DUMMY_PROJECT_ID, nf_analysis_input)

        assert result is mock_analysis
        mock_create.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            create_nextflow_with_custom_input_analysis=nf_analysis_input,
        )

    def test_passes_idempotency_key(self, mocker, configuration_fixture):
        """Verify idempotency_key is forwarded to the API call."""
        mock_analysis = MagicMock()

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_create = mocker.patch.object(
            ProjectAnalysisApi, "create_nextflow_analysis_with_custom_input",
            return_value=mock_analysis,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_nextflow_workflow,
        )

        nf_analysis_input = MagicMock()
        launch_nextflow_workflow(DUMMY_PROJECT_ID, nf_analysis_input, idempotency_key="nf-key-456")

        mock_create.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            create_nextflow_with_custom_input_analysis=nf_analysis_input,
            idempotency_key="nf-key-456",
        )

    def test_api_exception_propagated(self, mocker, configuration_fixture):
        """Verify ApiException is re-raised from the API call."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectAnalysisApi, "create_nextflow_analysis_with_custom_input",
            side_effect=ApiException("Launch failed"),
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            launch_nextflow_workflow,
        )

        with pytest.raises(ApiException):
            launch_nextflow_workflow(DUMMY_PROJECT_ID, MagicMock())


class TestGetAnalysisStorageIdFromAnalysisStorageSize:
    """Tests for get_analysis_storage_id_from_analysis_storage_size."""

    def test_returns_storage_id_string(self, mocker, configuration_fixture):
        """Verify the function returns the storage ID as a string."""
        mock_storage = MagicMock()
        mock_storage.id = "storage-id-12345"

        mocker.patch(
            f"{MODULE_PATH}.get_analysis_storage_from_analysis_storage_size",
            return_value=mock_storage,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            get_analysis_storage_id_from_analysis_storage_size,
        )

        result = get_analysis_storage_id_from_analysis_storage_size(
            DUMMY_PROJECT_ID, "Small"
        )

        assert result == "storage-id-12345"
        assert isinstance(result, str)


class TestConvertIcav2UrisToDataIdsFromCwlInputJson:
    """Tests for convert_icav2_uris_to_data_ids_from_cwl_input_json."""

    def test_delegates_to_convert_uris(self, mocker, configuration_fixture):
        """Verify the deprecated wrapper delegates to convert_uris_to_data_ids_from_cwl_input_json."""
        expected_result = ({"key": "value"}, [], [])

        mocker.patch(
            f"{MODULE_PATH}.convert_uris_to_data_ids_from_cwl_input_json",
            return_value=expected_result,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            convert_icav2_uris_to_data_ids_from_cwl_input_json,
        )

        input_obj = {"input_file": {"class": "File", "location": "icav2://proj/file.txt"}}
        result = convert_icav2_uris_to_data_ids_from_cwl_input_json(input_obj)

        assert result == expected_result


class TestListProjectPipelines:
    """Tests for list_project_pipelines."""

    def test_returns_pipeline_items(self, mocker, configuration_fixture):
        """Verify the function returns the items list from the API response."""
        mock_pipeline_1 = MagicMock()
        mock_pipeline_1.pipeline.id = "pipeline-1"
        mock_pipeline_1.pipeline.code = "code-1"

        mock_pipeline_2 = MagicMock()
        mock_pipeline_2.pipeline.id = "pipeline-2"
        mock_pipeline_2.pipeline.code = "code-2"

        mock_response = MagicMock()
        mock_response.items = [mock_pipeline_1, mock_pipeline_2]

        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get = mocker.patch.object(
            ProjectPipelineApi, "get_project_pipelines",
            return_value=mock_response,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            list_project_pipelines,
        )

        result = list_project_pipelines(DUMMY_PROJECT_ID)

        assert result == [mock_pipeline_1, mock_pipeline_2]
        assert len(result) == 2
        mock_get.assert_called_once_with(project_id=DUMMY_PROJECT_ID)

    def test_api_exception_raises_value_error(self, mocker, configuration_fixture):
        """Verify ApiException from the API is wrapped in ValueError."""
        mocker.patch(
            f"{MODULE_PATH}.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch.object(
            ProjectPipelineApi, "get_project_pipelines",
            side_effect=ApiException("Server error"),
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            list_project_pipelines,
        )

        with pytest.raises(ValueError):
            list_project_pipelines(DUMMY_PROJECT_ID)


class TestCreateCwlWorkflowFromZip:
    """Tests for create_cwl_workflow_from_zip."""

    def test_creates_pipeline_from_zip(self, mocker, configuration_fixture, tmp_path):
        """Verify the function extracts the zip and delegates to create_cwl_project_pipeline."""
        # Create a zip file with workflow.cwl inside
        zip_path = tmp_path / "my_workflow.zip"
        workflow_content = "class: Workflow\ndoc: Test workflow\n"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("my_workflow/workflow.cwl", workflow_content)
            zf.writestr("my_workflow/tools/tool1.cwl", "class: CommandLineTool\n")

        mock_pipeline_result = MagicMock()
        mock_pipeline_result.pipeline.id = DUMMY_PIPELINE_ID
        mock_pipeline_result.pipeline.code = "my-cwl-pipeline"

        mock_create = mocker.patch(
            f"{MODULE_PATH}.create_cwl_project_pipeline",
            return_value=mock_pipeline_result,
        )

        # Mock load_document_by_uri to return a mock workflow with doc attribute
        mock_workflow_obj = MagicMock()
        mock_workflow_obj.doc = "Test workflow description"
        mocker.patch(
            f"{MODULE_PATH}.load_document_by_uri",
            return_value=mock_workflow_obj,
        )

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            create_cwl_workflow_from_zip,
        )

        result = create_cwl_workflow_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_code="my-cwl-pipeline",
            zip_path=zip_path,
        )

        assert result is mock_pipeline_result
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["project_id"] == DUMMY_PROJECT_ID
        assert call_kwargs["pipeline_code"] == "my-cwl-pipeline"
        assert call_kwargs["workflow_description"] == "Test workflow description"

    def test_raises_file_not_found_without_workflow_cwl(self, mocker, configuration_fixture, tmp_path):
        """Verify FileNotFoundError is raised when zip lacks workflow.cwl."""
        zip_path = tmp_path / "bad_workflow.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("bad_workflow/some_other_file.cwl", "content")

        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            create_cwl_workflow_from_zip,
        )

        with pytest.raises(FileNotFoundError):
            create_cwl_workflow_from_zip(
                project_id=DUMMY_PROJECT_ID,
                pipeline_code="my-pipeline",
                zip_path=zip_path,
            )


class TestCreateParamsXml:
    """Tests for create_params_xml."""

    def test_creates_valid_xml_file(self, tmp_path):
        """Verify params.xml is created with expected XML structure."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            create_params_xml,
        )

        output_path = tmp_path / "params.xml"
        create_params_xml(inputs=[], output_path=output_path)

        assert output_path.exists()

        content = output_path.read_text()
        assert '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' in content
        assert "<pd:pipeline" in content
        assert "xmlns:pd=" in content
        assert "<pd:dataInputs/>" in content
        assert "<pd:steps/>" in content
        assert "</pd:pipeline>" in content


class TestPipelineUpdateFromZip:
    """Tests for pipeline_update_from_zip."""

    PIPELINES_MODULE_PATH = "wrapica.pipelines"
    USER_MODULE_PATH = "wrapica.user"
    DUMMY_FILE_ID_1 = "11111111-aaaa-4000-8000-111111111111"
    DUMMY_FILE_ID_2 = "22222222-bbbb-4000-8000-222222222222"
    DUMMY_FILE_ID_3 = "33333333-cccc-4000-8000-333333333333"

    @pytest.fixture
    def draft_pipeline_mock(self, mocker):
        """Create a mock draft pipeline owned by the current user."""
        from tests.test_helpers import DUMMY_USER_ID

        mock_pipeline = MagicMock()
        mock_pipeline.pipeline.status = "DRAFT"
        mock_pipeline.pipeline.owner.id = DUMMY_USER_ID
        mocker.patch(
            f"{MODULE_PATH}.get_project_pipeline_obj",
            return_value=mock_pipeline,
        )
        return mock_pipeline

    @pytest.fixture
    def mock_user_id(self, mocker):
        """Mock the user ID returned from configuration."""
        from tests.test_helpers import DUMMY_USER_ID

        mocker.patch(
            f"{self.USER_MODULE_PATH}.get_user_id_from_configuration",
            return_value=DUMMY_USER_ID,
        )
        return DUMMY_USER_ID

    @pytest.fixture
    def mock_download_pipeline(self, mocker):
        """Mock download_pipeline_to_directory to write files into the temp dir."""
        def _download(pipeline_id, output_directory, file_contents=None):
            """Helper to set up the download mock with specific file contents.

            Args:
                file_contents: dict mapping relative path strings to file content.
            """
            if file_contents is None:
                file_contents = {}

            def side_effect(pid, output_dir):
                for rel_path, content in file_contents.items():
                    fpath = output_dir / rel_path
                    fpath.parent.mkdir(parents=True, exist_ok=True)
                    fpath.write_text(content)

            return mocker.patch(
                f"{self.PIPELINES_MODULE_PATH}.download_pipeline_to_directory",
                side_effect=side_effect,
            )

        return _download

    @pytest.fixture
    def mock_pipeline_files(self, mocker):
        """Mock list_pipeline_files to return a set of PipelineFile objects."""
        def _make(file_mapping):
            """file_mapping: dict of {relative_path_str: file_id}"""
            pipeline_files = []
            for name, fid in file_mapping.items():
                pf = MagicMock()
                pf.name = name
                pf.id = fid
                pipeline_files.append(pf)
            return mocker.patch(
                f"{self.PIPELINES_MODULE_PATH}.list_pipeline_files",
                return_value=pipeline_files,
            )

        return _make

    @pytest.fixture
    def mock_update_delete_add(self, mocker):
        """Mock the update, delete, and add pipeline file functions."""
        mock_update = mocker.patch(f"{MODULE_PATH}.update_pipeline_file")
        mock_delete = mocker.patch(f"{MODULE_PATH}.delete_pipeline_file")
        mock_add = mocker.patch(f"{MODULE_PATH}.add_pipeline_file")
        return mock_update, mock_delete, mock_add

    def _create_zip(self, tmp_path, zip_name, file_contents):
        """Helper to create a zip file with given contents.

        Args:
            tmp_path: pytest tmp_path fixture
            zip_name: name of the zip file (without .zip extension)
            file_contents: dict mapping paths relative to zip root dir to content strings

        Returns:
            Path to the created zip file.
        """
        zip_path = tmp_path / f"{zip_name}.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for rel_path, content in file_contents.items():
                zf.writestr(f"{zip_name}/{rel_path}", content)
        return zip_path

    def test_raises_file_not_found_for_missing_zip(self, configuration_fixture):
        """Verify FileNotFoundError when zip_path doesn't exist."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        with pytest.raises(FileNotFoundError, match="does not exist"):
            pipeline_update_from_zip(
                project_id=DUMMY_PROJECT_ID,
                pipeline_id=DUMMY_PIPELINE_ID,
                zip_path=Path("/nonexistent/pipeline.zip"),
                force=True,
            )

    def test_raises_value_error_for_non_zip_file(self, tmp_path, configuration_fixture):
        """Verify ValueError when file doesn't have .zip extension."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        non_zip = tmp_path / "pipeline.tar.gz"
        non_zip.write_text("not a zip")

        with pytest.raises(ValueError, match="Expected a .zip file"):
            pipeline_update_from_zip(
                project_id=DUMMY_PROJECT_ID,
                pipeline_id=DUMMY_PIPELINE_ID,
                zip_path=non_zip,
                force=True,
            )

    def test_raises_value_error_for_non_draft_pipeline(
        self, mocker, tmp_path, configuration_fixture, mock_user_id
    ):
        """Verify ValueError when pipeline is not in DRAFT status."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        mock_pipeline = MagicMock()
        mock_pipeline.pipeline.status = "RELEASED"
        mock_pipeline.pipeline.owner.id = mock_user_id
        mocker.patch(
            f"{MODULE_PATH}.get_project_pipeline_obj",
            return_value=mock_pipeline,
        )

        zip_path = self._create_zip(tmp_path, "my_pipeline", {"main.nf": "process {}"})

        with pytest.raises(ValueError, match="must be DRAFT"):
            pipeline_update_from_zip(
                project_id=DUMMY_PROJECT_ID,
                pipeline_id=DUMMY_PIPELINE_ID,
                zip_path=zip_path,
                force=True,
            )

    def test_raises_value_error_for_non_owner(
        self, mocker, tmp_path, configuration_fixture
    ):
        """Verify ValueError when current user doesn't own the pipeline."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        mock_pipeline = MagicMock()
        mock_pipeline.pipeline.status = "DRAFT"
        mock_pipeline.pipeline.owner.id = "other-user-id-0000-0000-000000000000"
        mocker.patch(
            f"{MODULE_PATH}.get_project_pipeline_obj",
            return_value=mock_pipeline,
        )
        mocker.patch(
            f"{self.USER_MODULE_PATH}.get_user_id_from_configuration",
            return_value="eeeeeeee-5555-4000-8000-eeeeeeeeeeee",
        )

        zip_path = self._create_zip(tmp_path, "my_pipeline", {"main.nf": "process {}"})

        with pytest.raises(ValueError, match="not owned by the current user"):
            pipeline_update_from_zip(
                project_id=DUMMY_PROJECT_ID,
                pipeline_id=DUMMY_PIPELINE_ID,
                zip_path=zip_path,
                force=True,
            )

    def test_no_changes_detected(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify returns empty lists when zip matches ICAv2 contents exactly."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        files = {"main.cwl": "class: Workflow\n", "tools/tool1.cwl": "class: CommandLineTool\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=files)
        mock_pipeline_files({
            "main.cwl": self.DUMMY_FILE_ID_1,
            "tools/tool1.cwl": self.DUMMY_FILE_ID_2,
        })

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert result == {"edited": [], "added": [], "deleted": []}
        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_update.assert_not_called()
        mock_delete.assert_not_called()
        mock_add.assert_not_called()

    def test_edited_files_are_updated(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify edited files trigger update_pipeline_file calls."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        # Local zip has updated content
        local_files = {"main.cwl": "class: Workflow\ndoc: Updated\n"}
        # ICAv2 has old content
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert len(result["edited"]) == 1
        assert Path("main.cwl") in result["edited"]
        assert result["added"] == []
        assert result["deleted"] == []

        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_update.assert_called_once()
        # Verify the file_id was resolved correctly
        call_args = mock_update.call_args
        assert call_args[0][2] == self.DUMMY_FILE_ID_1  # file_id argument

    def test_new_files_are_added(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify new files in the zip trigger add_pipeline_file calls."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        # Local zip has an extra file
        local_files = {
            "main.cwl": "class: Workflow\n",
            "tools/new_tool.cwl": "class: CommandLineTool\n",
        }
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert len(result["added"]) == 1
        assert Path("tools/new_tool.cwl") in result["added"]
        assert result["edited"] == []
        assert result["deleted"] == []

        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_add.assert_called_once()

    def test_deleted_files_are_removed(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify files missing from the zip trigger delete_pipeline_file calls."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        # Local zip has fewer files
        local_files = {"main.cwl": "class: Workflow\n"}
        # ICAv2 has an extra file
        icav2_files = {
            "main.cwl": "class: Workflow\n",
            "tools/old_tool.cwl": "class: CommandLineTool\n",
        }

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({
            "main.cwl": self.DUMMY_FILE_ID_1,
            "tools/old_tool.cwl": self.DUMMY_FILE_ID_2,
        })

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert len(result["deleted"]) == 1
        assert Path("tools/old_tool.cwl") in result["deleted"]
        assert result["edited"] == []
        assert result["added"] == []

        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_delete.assert_called_once()
        call_args = mock_delete.call_args
        assert call_args[0][2] == self.DUMMY_FILE_ID_2

    def test_combined_edits_adds_and_deletes(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify a mixed scenario with edits, adds, and deletes all in one call."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        local_files = {
            "main.cwl": "class: Workflow\ndoc: changed\n",  # edited
            "tools/new_tool.cwl": "class: CommandLineTool\n",  # new
        }
        icav2_files = {
            "main.cwl": "class: Workflow\n",  # will show as edited
            "tools/old_tool.cwl": "class: CommandLineTool\n",  # will be deleted
        }

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({
            "main.cwl": self.DUMMY_FILE_ID_1,
            "tools/old_tool.cwl": self.DUMMY_FILE_ID_2,
        })

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert Path("main.cwl") in result["edited"]
        assert Path("tools/new_tool.cwl") in result["added"]
        assert Path("tools/old_tool.cwl") in result["deleted"]

        mock_update, mock_delete, mock_add = mock_update_delete_add
        assert mock_update.call_count == 1
        assert mock_add.call_count == 1
        assert mock_delete.call_count == 1

    def test_hidden_files_excluded_from_adds(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify hidden directories and files are excluded from additions."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        local_files = {
            "main.cwl": "class: Workflow\n",
            ".git/config": "git config",
            ".hidden_file": "secret",
        }
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        # Hidden files/dirs should be excluded
        assert result == {"edited": [], "added": [], "deleted": []}

    def test_test_and_meta_files_excluded_from_adds(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify .test, .test.snap, and environment/meta yaml files are excluded."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        local_files = {
            "main.cwl": "class: Workflow\n",
            "tools/tool.test": "test content",
            "tools/tool.test.snap": "snapshot",
            "environment.yml": "dependencies: []",
            "meta.yaml": "name: test",
        }
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        assert result == {"edited": [], "added": [], "deleted": []}

    def test_nextflow_pipeline_generates_icav2_config(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify Nextflow pipelines get conf/icav2.config auto-generated if missing."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        # Mock Nextflow helper to avoid full implementation complexity
        mock_include = mocker.patch(
            f"{MODULE_PATH}.include_icav2_config_into_nextflow_config"
        )
        mocker.patch(
            f"{MODULE_PATH}.get_default_icav2_config_content",
            return_value="// icav2 config placeholder\n",
        )

        # Nextflow pipeline with nextflow.config but no conf/icav2.config
        local_files = {"nextflow.config": "params { input = '' }\n", "main.nf": "process foo {}"}
        icav2_files = {"nextflow.config": "params { input = '' }\n", "main.nf": "process foo {}"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({
            "nextflow.config": self.DUMMY_FILE_ID_1,
            "main.nf": self.DUMMY_FILE_ID_2,
        })

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        # The icav2.config injection helper should have been called
        mock_include.assert_called_once()

    def test_icav2_config_not_deleted_from_icav2(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify conf/icav2.config is never deleted even if missing from the local zip."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        # Local zip has no conf/icav2.config
        local_files = {"main.cwl": "class: Workflow\n"}
        # ICAv2 has conf/icav2.config
        icav2_files = {
            "main.cwl": "class: Workflow\n",
            "conf/icav2.config": "// generated config\n",
        }

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({
            "main.cwl": self.DUMMY_FILE_ID_1,
            "conf/icav2.config": self.DUMMY_FILE_ID_2,
        })

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=True,
        )

        # conf/icav2.config should NOT appear in deleted
        assert Path("conf/icav2.config") not in result["deleted"]
        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_delete.assert_not_called()

    def test_force_false_cancels_on_user_input(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify update is cancelled when user declines the confirmation prompt."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        local_files = {"main.cwl": "class: Workflow\ndoc: changed\n"}
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        # Simulate user declining
        mocker.patch("builtins.input", return_value="n")

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=False,
        )

        assert result == {"edited": [], "added": [], "deleted": []}
        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_update.assert_not_called()

    def test_force_false_proceeds_on_user_confirm(
        self,
        mocker,
        tmp_path,
        configuration_fixture,
        draft_pipeline_mock,
        mock_user_id,
        mock_download_pipeline,
        mock_pipeline_files,
        mock_update_delete_add,
    ):
        """Verify update proceeds when user confirms the prompt."""
        from wrapica.project_pipelines.functions.project_pipelines_functions import (
            pipeline_update_from_zip,
        )

        local_files = {"main.cwl": "class: Workflow\ndoc: changed\n"}
        icav2_files = {"main.cwl": "class: Workflow\n"}

        zip_path = self._create_zip(tmp_path, "my_pipeline", local_files)
        mock_download_pipeline(DUMMY_PIPELINE_ID, None, file_contents=icav2_files)
        mock_pipeline_files({"main.cwl": self.DUMMY_FILE_ID_1})

        # Simulate user confirming
        mocker.patch("builtins.input", return_value="yes")

        result = pipeline_update_from_zip(
            project_id=DUMMY_PROJECT_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
            zip_path=zip_path,
            force=False,
        )

        assert len(result["edited"]) == 1
        mock_update, mock_delete, mock_add = mock_update_delete_add
        mock_update.assert_called_once()
