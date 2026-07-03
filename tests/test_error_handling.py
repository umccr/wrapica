"""Cross-cutting error handling tests for wrapica.

This module tests error cases that aren't already covered in existing
domain-specific test modules. Specifically:
- FileNotFoundError from get_project_data_obj_from_project_id_and_path (empty results)
- StopIteration from get_pipeline_obj_from_pipeline_code (code not found)

Already covered elsewhere (not duplicated here):
- ApiException propagation from ProjectApi.get_project (test_project.py)
- StopIteration from get_project_obj_from_project_name (test_project.py)
- ValueError from get_project_id (test_project.py)
- ApiException propagation from DataApi.get_data (test_data.py)
- ApiException propagation from PipelineApi.get_pipeline (test_pipelines.py)
- EnvironmentError from get_project_id_from_env_var (test_configuration.py)
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from libica.openapi.v3.api.project_data_api import ProjectDataApi

from tests.test_helpers import DUMMY_PROJECT_ID, DUMMY_PIPELINE_ID


class TestGetProjectDataObjFromProjectIdAndPathFileNotFound:
    """Test FileNotFoundError from get_project_data_obj_from_project_id_and_path."""

    def test_raises_file_not_found_error_when_listing_returns_no_match(
        self, mocker, configuration_fixture
    ):
        """Verify FileNotFoundError is raised when the listing returns no matching items.

        When get_project_data_file_id_from_project_id_and_path calls the API and
        filters for a matching path, if no items match and create_data_if_not_found
        is False, a FileNotFoundError should be raised.

        Validates: Requirements 17.1, 17.4
        """
        mocker.patch(
            "wrapica.project_data.functions.project_data_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        # Mock the API to return an empty items list
        mock_response = MagicMock()
        mock_response.items = []

        mocker.patch.object(
            ProjectDataApi,
            "get_project_data_list",
            return_value=mock_response,
        )

        from wrapica.project_data import get_project_data_obj_from_project_id_and_path

        with pytest.raises(FileNotFoundError):
            get_project_data_obj_from_project_id_and_path(
                project_id=DUMMY_PROJECT_ID,
                data_path=Path("/path/to/nonexistent_file.txt"),
                data_type="FILE",
                create_data_if_not_found=False,
            )


class TestGetPipelineObjFromPipelineCodeStopIteration:
    """Test StopIteration from get_pipeline_obj_from_pipeline_code."""

    def test_raises_stop_iteration_when_code_not_found(self, mocker, configuration_fixture):
        """Verify StopIteration is raised when no pipeline matches the given code.

        Validates: Requirements 17.2
        """
        # Mock list_all_pipelines to return pipelines that don't match
        mock_pipeline = MagicMock()
        mock_pipeline.code = "other-pipeline-code"
        mock_pipeline.id = DUMMY_PIPELINE_ID

        mocker.patch(
            "wrapica.pipelines.functions.pipelines_functions.list_all_pipelines",
            return_value=[mock_pipeline],
        )

        from wrapica.pipelines import get_pipeline_obj_from_pipeline_code

        with pytest.raises(StopIteration):
            get_pipeline_obj_from_pipeline_code("nonexistent-pipeline-code")
