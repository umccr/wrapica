"""Tests for wrapica.project_analysis module."""
import json

import pytest
from unittest.mock import MagicMock

from tests.test_helpers import (
    DUMMY_PROJECT_ID,
    DUMMY_ANALYSIS_ID,
    make_paged_response,
)


class TestGetAnalysisObjFromAnalysisId:
    """Tests for get_analysis_obj_from_analysis_id."""

    def test_returns_analysis_object(self, mocker):
        """
        WHEN get_analysis_obj_from_analysis_id is called with valid IDs,
        THEN it returns the AnalysisV4 object from ProjectAnalysisApi.get_analysis.

        Validates: Requirements 5.1
        """
        mock_analysis = MagicMock()
        mock_analysis.id = DUMMY_ANALYSIS_ID
        mock_analysis.status = "SUCCEEDED"

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_get_analysis = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.get_analysis",
            return_value=mock_analysis,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_analysis_obj_from_analysis_id,
        )

        result = get_analysis_obj_from_analysis_id(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
        )

        assert result is mock_analysis
        mock_get_analysis.assert_called_once_with(
            project_id=str(DUMMY_PROJECT_ID),
            analysis_id=str(DUMMY_ANALYSIS_ID),
        )


class TestListAnalyses:
    """Tests for list_analyses with multi-page pagination."""

    def test_list_analyses_multi_page(self, mocker):
        """
        WHEN list_analyses is called and the API returns multiple pages via next_page_token,
        THEN it aggregates all items across pages.

        Validates: Requirements 5.2
        """
        analysis_a = MagicMock()
        analysis_b = MagicMock()
        analysis_c = MagicMock()

        pages = make_paged_response(
            items_per_page=[[analysis_a, analysis_b], [analysis_c]],
            use_page_token=True,
        )

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_search = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.search_analyses",
            side_effect=pages,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            list_analyses,
        )

        result = list_analyses(project_id=DUMMY_PROJECT_ID)

        assert len(result) == 3
        assert result == [analysis_a, analysis_b, analysis_c]
        assert mock_search.call_count == 2


class TestGetAnalysisSteps:
    """Tests for get_analysis_steps with include_technical_steps flag."""

    def _make_step(self, name, technical):
        step = MagicMock()
        step.name = name
        step.technical = technical
        return step

    def test_excludes_technical_steps_when_false(self, mocker):
        """
        WHEN get_analysis_steps is called with include_technical_steps=False,
        THEN technical steps are filtered out.

        Validates: Requirements 5.3
        """
        step_normal = self._make_step("run_step", False)
        step_tech = self._make_step("init_step", True)

        mock_response = MagicMock()
        mock_response.items = [step_normal, step_tech]

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.get_analysis_steps",
            return_value=mock_response,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_analysis_steps,
        )

        result = get_analysis_steps(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
            include_technical_steps=False,
        )

        assert len(result) == 1
        assert result[0] is step_normal

    def test_includes_technical_steps_when_true(self, mocker):
        """
        WHEN get_analysis_steps is called with include_technical_steps=True,
        THEN all steps (including technical) are returned.

        Validates: Requirements 5.3
        """
        step_normal = self._make_step("run_step", False)
        step_tech = self._make_step("init_step", True)

        mock_response = MagicMock()
        mock_response.items = [step_normal, step_tech]

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.get_analysis_steps",
            return_value=mock_response,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_analysis_steps,
        )

        result = get_analysis_steps(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
            include_technical_steps=True,
        )

        assert len(result) == 2
        assert step_tech in result


class TestGetAnalysisLogFromAnalysisStep:
    """Tests for get_analysis_log_from_analysis_step."""

    def test_returns_logs_attribute(self):
        """
        WHEN get_analysis_log_from_analysis_step is called with an AnalysisStep,
        THEN it returns the .logs attribute of that step.

        Validates: Requirements 5.4
        """
        mock_step = MagicMock()
        mock_logs = MagicMock()
        mock_step.logs = mock_logs

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_analysis_log_from_analysis_step,
        )

        result = get_analysis_log_from_analysis_step(mock_step)

        assert result is mock_logs


class TestAbortAnalysis:
    """Tests for abort_analysis."""

    def test_abort_analysis_calls_api(self, mocker):
        """
        WHEN abort_analysis is called with valid project and analysis IDs,
        THEN it delegates to ProjectAnalysisApi.abort_analysis.

        Validates: Requirements 5.5
        """
        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mock_abort = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.abort_analysis",
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            abort_analysis,
        )

        abort_analysis(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
        )

        mock_abort.assert_called_once_with(
            project_id=str(DUMMY_PROJECT_ID),
            analysis_id=str(DUMMY_ANALYSIS_ID),
        )


class TestGetProjectAnalysisInputs:
    """Tests for get_project_analysis_inputs."""

    def test_returns_items_from_api_response(self, mocker):
        """
        WHEN get_project_analysis_inputs is called,
        THEN it returns the .items from the API response.

        Validates: Requirements 5.6
        """
        mock_input_a = MagicMock()
        mock_input_b = MagicMock()

        mock_response = MagicMock()
        mock_response.items = [mock_input_a, mock_input_b]

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.get_analysis_inputs",
            return_value=mock_response,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_project_analysis_inputs,
        )

        result = get_project_analysis_inputs(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
        )

        assert result == [mock_input_a, mock_input_b]


class TestGetCwlAnalysisInputJson:
    """Tests for get_cwl_analysis_input_json."""

    def test_returns_parsed_dict(self, mocker):
        """
        WHEN get_cwl_analysis_input_json is called,
        THEN it parses the input_json field and returns a dictionary.

        Validates: Requirements 5.7
        """
        expected_dict = {"input_file": {"class": "File", "location": "icav2://bucket/file.bam"}}

        mock_response = MagicMock()
        mock_response.input_json = json.dumps(expected_dict)

        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.ProjectAnalysisApi.get_cwl_input_json",
            return_value=mock_response,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            get_cwl_analysis_input_json,
        )

        result = get_cwl_analysis_input_json(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
        )

        assert result == expected_dict
        assert isinstance(result, dict)


class TestCoerceAnalysisIdOrUserReferenceToAnalysisObj:
    """Tests for coerce_analysis_id_or_user_reference_to_analysis_obj."""

    def test_uuid_delegates_to_id_lookup(self, mocker):
        """
        WHEN coerce_analysis_id_or_user_reference_to_analysis_obj is called with a UUID,
        THEN it delegates to get_analysis_obj_from_analysis_id.

        Validates: Requirements 5.8
        """
        mock_analysis = MagicMock()
        mock_analysis.id = DUMMY_ANALYSIS_ID

        mock_id_lookup = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_analysis_obj_from_analysis_id",
            return_value=mock_analysis,
        )
        mock_ref_lookup = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_analysis_obj_from_user_reference",
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            coerce_analysis_id_or_user_reference_to_analysis_obj,
        )

        result = coerce_analysis_id_or_user_reference_to_analysis_obj(
            project_id=DUMMY_PROJECT_ID,
            analysis_id_or_user_reference=DUMMY_ANALYSIS_ID,
        )

        assert result is mock_analysis
        mock_id_lookup.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            analysis_id=DUMMY_ANALYSIS_ID,
        )
        mock_ref_lookup.assert_not_called()

    def test_non_uuid_delegates_to_user_reference_lookup(self, mocker):
        """
        WHEN coerce_analysis_id_or_user_reference_to_analysis_obj is called with a non-UUID string,
        THEN it delegates to get_analysis_obj_from_user_reference.

        Validates: Requirements 5.9
        """
        user_ref = "my-analysis-user-ref"
        mock_analysis = MagicMock()
        mock_analysis.id = DUMMY_ANALYSIS_ID

        mock_id_lookup = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_analysis_obj_from_analysis_id",
        )
        mock_ref_lookup = mocker.patch(
            "wrapica.project_analysis.functions.project_analysis_functions.get_analysis_obj_from_user_reference",
            return_value=mock_analysis,
        )

        from wrapica.project_analysis.functions.project_analysis_functions import (
            coerce_analysis_id_or_user_reference_to_analysis_obj,
        )

        result = coerce_analysis_id_or_user_reference_to_analysis_obj(
            project_id=DUMMY_PROJECT_ID,
            analysis_id_or_user_reference=user_ref,
        )

        assert result is mock_analysis
        mock_ref_lookup.assert_called_once_with(
            project_id=DUMMY_PROJECT_ID,
            user_reference=user_ref,
        )
        mock_id_lookup.assert_not_called()
