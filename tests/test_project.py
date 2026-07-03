"""Tests for wrapica.project functions."""
import pytest
from unittest.mock import MagicMock

from libica.openapi.v3 import ApiException

from tests.test_helpers import DUMMY_PROJECT_ID, make_paged_response


class TestListProjects:
    """Tests for list_projects."""

    def test_multi_page_pagination(self, mocker, configuration_fixture):
        """Verify list_projects collects items from multiple pages using total_item_count."""
        project_a = MagicMock()
        project_a.id = DUMMY_PROJECT_ID
        project_a.name = "project-a"

        project_b = MagicMock()
        project_b.id = "bbbbbbbb-2222-4000-8000-bbbbbbbbbbbb"
        project_b.name = "project-b"

        project_c = MagicMock()
        project_c.id = "cccccccc-3333-4000-8000-cccccccccccc"
        project_c.name = "project-c"

        pages = make_paged_response(
            [[project_a, project_b], [project_c]],
            use_page_token=False,
        )

        # Override page size to 2 so pagination triggers
        mocker.patch(
            "wrapica.project.functions.project_functions.LIBICAV2_DEFAULT_PAGE_SIZE",
            2,
        )

        # Patch get_icav2_configuration where it's imported in project_functions
        mocker.patch(
            "wrapica.project.functions.project_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_projects",
            side_effect=pages,
        )

        from wrapica.project.functions.project_functions import list_projects

        result = list_projects()

        assert len(result) == 3
        assert result == [project_a, project_b, project_c]


class TestGetProjectObjFromProjectId:
    """Tests for get_project_obj_from_project_id."""

    def test_returns_project_for_valid_id(self, mocker, configuration_fixture):
        """Verify the function returns the Project object from ProjectApi.get_project."""
        mock_project = MagicMock()
        mock_project.id = DUMMY_PROJECT_ID
        mock_project.name = "test-project"

        mocker.patch(
            "wrapica.project.functions.project_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_project",
            return_value=mock_project,
        )

        from wrapica.project.functions.project_functions import get_project_obj_from_project_id

        result = get_project_obj_from_project_id(DUMMY_PROJECT_ID)

        assert result == mock_project
        assert result.id == DUMMY_PROJECT_ID

    def test_propagates_api_exception(self, mocker, configuration_fixture):
        """Verify ApiException from ProjectApi.get_project is propagated."""
        mocker.patch(
            "wrapica.project.functions.project_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_project",
            side_effect=ApiException("Not found"),
        )

        from wrapica.project.functions.project_functions import get_project_obj_from_project_id

        with pytest.raises(ApiException):
            get_project_obj_from_project_id(DUMMY_PROJECT_ID)


class TestGetProjectObjFromProjectName:
    """Tests for get_project_obj_from_project_name."""

    def test_matching_name_returns_project(self, mocker, configuration_fixture):
        """Verify the correct project is returned when name matches."""
        mock_project = MagicMock()
        mock_project.id = DUMMY_PROJECT_ID
        mock_project.name = "my-project"

        mocker.patch(
            "wrapica.project.functions.project_functions.list_projects",
            return_value=[mock_project],
        )

        from wrapica.project.functions.project_functions import get_project_obj_from_project_name

        result = get_project_obj_from_project_name("my-project")

        assert result == mock_project
        assert result.name == "my-project"

    def test_non_matching_name_raises_stop_iteration(self, mocker, configuration_fixture):
        """Verify StopIteration is raised when no project matches the name."""
        mock_project = MagicMock()
        mock_project.name = "other-project"

        mocker.patch(
            "wrapica.project.functions.project_functions.list_projects",
            return_value=[mock_project],
        )

        from wrapica.project.functions.project_functions import get_project_obj_from_project_name

        with pytest.raises(StopIteration):
            get_project_obj_from_project_name("nonexistent-project")


class TestCoerceProjectIdOrNameToProjectObj:
    """Tests for coerce_project_id_or_name_to_project_obj."""

    def test_uuid_delegates_to_id_lookup(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_project_obj_from_project_id."""
        mock_project = MagicMock()
        mock_project.id = DUMMY_PROJECT_ID

        mock_fn = mocker.patch(
            "wrapica.project.functions.project_functions.get_project_obj_from_project_id",
            return_value=mock_project,
        )

        from wrapica.project.functions.project_functions import coerce_project_id_or_name_to_project_obj

        result = coerce_project_id_or_name_to_project_obj(DUMMY_PROJECT_ID)

        mock_fn.assert_called_once_with(DUMMY_PROJECT_ID)
        assert result == mock_project

    def test_non_uuid_delegates_to_name_lookup(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_project_obj_from_project_name."""
        mock_project = MagicMock()
        mock_project.name = "my-project"

        mock_fn = mocker.patch(
            "wrapica.project.functions.project_functions.get_project_obj_from_project_name",
            return_value=mock_project,
        )

        from wrapica.project.functions.project_functions import coerce_project_id_or_name_to_project_obj

        result = coerce_project_id_or_name_to_project_obj("my-project")

        mock_fn.assert_called_once_with("my-project")
        assert result == mock_project


class TestGetProjectId:
    """Tests for get_project_id."""

    def test_returns_env_var_when_set(self, mocker, configuration_fixture):
        """Verify get_project_id returns ICAV2_PROJECT_ID env var value."""
        from wrapica.project.functions.project_functions import get_project_id

        # conftest already sets ICAV2_PROJECT_ID to "00000000-0000-4000-8000-000000000000"
        result = get_project_id()

        assert result == "00000000-0000-4000-8000-000000000000"

    def test_raises_value_error_without_env_var_or_session(self, monkeypatch, mocker, configuration_fixture):
        """Verify ValueError is raised when both env var and session file are unavailable."""
        monkeypatch.delenv("ICAV2_PROJECT_ID", raising=False)

        mocker.patch(
            "wrapica.project.functions.project_functions.get_project_id_from_env_var",
            side_effect=EnvironmentError("No env var"),
        )
        mocker.patch(
            "wrapica.project.functions.project_functions.get_project_id_from_session_file",
            side_effect=KeyError("No session file"),
        )

        from wrapica.project.functions.project_functions import get_project_id

        with pytest.raises(ValueError):
            get_project_id()


class TestCheckProjectHasDataSharingEnabled:
    """Tests for check_project_has_data_sharing_enabled."""

    def test_returns_true_when_enabled(self, mocker, configuration_fixture):
        """Verify function returns True when data_sharing_enabled is True."""
        mock_project = MagicMock()
        mock_project.data_sharing_enabled = True

        mocker.patch(
            "wrapica.project.functions.project_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_project",
            return_value=mock_project,
        )

        from wrapica.project.functions.project_functions import check_project_has_data_sharing_enabled

        result = check_project_has_data_sharing_enabled(DUMMY_PROJECT_ID)

        assert result is True

    def test_returns_false_when_disabled(self, mocker, configuration_fixture):
        """Verify function returns False when data_sharing_enabled is False."""
        mock_project = MagicMock()
        mock_project.data_sharing_enabled = False

        mocker.patch(
            "wrapica.project.functions.project_functions.get_icav2_configuration",
            return_value=configuration_fixture,
        )

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_project",
            return_value=mock_project,
        )

        from wrapica.project.functions.project_functions import check_project_has_data_sharing_enabled

        result = check_project_has_data_sharing_enabled(DUMMY_PROJECT_ID)

        assert result is False


class TestGetProjectNameFromProjectId:
    """Tests for get_project_name_from_project_id."""

    def test_returns_name_from_mapping(self, mocker, configuration_fixture):
        """Verify function returns project name from the mapping dict."""
        mock_mapping = {
            DUMMY_PROJECT_ID: "my-project",
            "bbbbbbbb-2222-4000-8000-bbbbbbbbbbbb": "other-project",
        }

        mocker.patch(
            "wrapica.project.functions.project_functions._get_project_mapping_dict",
            return_value=mock_mapping,
        )

        from wrapica.project.functions.project_functions import get_project_name_from_project_id

        result = get_project_name_from_project_id(DUMMY_PROJECT_ID)

        assert result == "my-project"
