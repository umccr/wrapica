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
