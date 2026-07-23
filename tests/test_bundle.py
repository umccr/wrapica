"""Tests for wrapica.bundle functions."""
import pytest
from unittest.mock import MagicMock

from tests.test_helpers import (
    DUMMY_BUNDLE_ID,
    DUMMY_PIPELINE_ID,
    DUMMY_PROJECT_ID,
    DUMMY_REGION_ID,
)


class TestGenerateEmptyBundle:
    """Tests for generate_empty_bundle."""

    def test_creates_bundle_with_correct_payload(self, mocker, configuration_fixture):
        """Verify CreateBundle payload contains provided name, version, description, comment, region, and categories."""
        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID
        mock_bundle.name = "test-bundle"

        mock_create = mocker.patch(
            "libica.openapi.v3.api.bundle_api.BundleApi.create_bundle",
            return_value=mock_bundle,
        )

        from wrapica.bundle.functions.bundle_functions import generate_empty_bundle

        result = generate_empty_bundle(
            bundle_name="test-bundle",
            bundle_version="1.0.0",
            short_description="A test bundle",
            version_comment="Initial version",
            region_id=DUMMY_REGION_ID,
            categories=["Test", "CI"],
        )

        assert result == mock_bundle
        mock_create.assert_called_once()
        create_bundle_arg = mock_create.call_args[1]["create_bundle"]
        assert create_bundle_arg.name == "test-bundle"
        assert create_bundle_arg.short_description == "A test bundle"
        assert create_bundle_arg.bundle_release_version == "1.0.0"
        assert create_bundle_arg.bundle_version_comment == "Initial version"
        assert create_bundle_arg.bundle_status == "DRAFT"
        assert create_bundle_arg.categories == ["Test", "CI"]

    def test_default_region_used_when_region_id_none(self, mocker, configuration_fixture):
        """Verify get_default_region is called when region_id is not provided."""
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID

        # Patch get_regions so set_default_region/get_default_region work
        mocker.patch(
            "libica.openapi.v3.api.region_api.RegionApi.get_regions",
            return_value=MagicMock(items=[mock_region]),
        )

        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID

        mock_create = mocker.patch(
            "libica.openapi.v3.api.bundle_api.BundleApi.create_bundle",
            return_value=mock_bundle,
        )

        from wrapica.bundle.functions.bundle_functions import generate_empty_bundle

        result = generate_empty_bundle(
            bundle_name="test-bundle",
            bundle_version="1.0.0",
            short_description="A test bundle",
            version_comment="Initial version",
            categories=[],
        )

        assert result == mock_bundle
        mock_create.assert_called_once()


class TestGetBundleObjFromBundleId:
    """Tests for get_bundle_obj_from_bundle_id."""

    def test_returns_bundle_matching_id(self, mocker, configuration_fixture):
        """Verify the Bundle object returned matches the mocked response."""
        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID
        mock_bundle.name = "my-bundle"

        mock_get = mocker.patch(
            "libica.openapi.v3.api.bundle_api.BundleApi.get_bundle",
            return_value=mock_bundle,
        )

        from wrapica.bundle.functions.bundle_functions import get_bundle_obj_from_bundle_id

        result = get_bundle_obj_from_bundle_id(DUMMY_BUNDLE_ID)

        assert result == mock_bundle
        mock_get.assert_called_once_with(bundle_id=DUMMY_BUNDLE_ID)


class TestGetBundleObjFromBundleName:
    """Tests for get_bundle_obj_from_bundle_name."""

    def test_returns_bundle_from_single_result(self, mocker, configuration_fixture):
        """Verify function returns the bundle when filter_bundles returns one result."""
        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID
        mock_bundle.name = "my-bundle"

        mocker.patch(
            "wrapica.bundle.functions.bundle_functions.filter_bundles",
            return_value=[mock_bundle],
        )

        from wrapica.bundle.functions.bundle_functions import get_bundle_obj_from_bundle_name

        result = get_bundle_obj_from_bundle_name("my-bundle")

        assert result == mock_bundle

    def test_empty_results_raises_index_error(self, mocker, configuration_fixture):
        """Verify IndexError is raised when filter_bundles returns empty list."""
        mocker.patch(
            "wrapica.bundle.functions.bundle_functions.filter_bundles",
            return_value=[],
        )

        from wrapica.bundle.functions.bundle_functions import get_bundle_obj_from_bundle_name

        with pytest.raises(IndexError):
            get_bundle_obj_from_bundle_name("nonexistent-bundle")


class TestAddPipelineToBundle:
    """Tests for add_pipeline_to_bundle."""

    def test_released_pipeline_calls_link_api(self, mocker, configuration_fixture):
        """Verify link_pipeline_to_bundle is called when pipeline status is RELEASED."""
        mock_pipeline = MagicMock()
        mock_pipeline.status = "RELEASED"

        mocker.patch(
            "wrapica.bundle.functions.bundle_functions.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID

        mocker.patch(
            "wrapica.bundle.functions.bundle_functions.get_bundle_obj_from_bundle_id",
            return_value=mock_bundle,
        )

        mock_link = mocker.patch(
            "libica.openapi.v3.api.bundle_pipeline_api.BundlePipelineApi.link_pipeline_to_bundle",
            return_value=None,
        )

        from wrapica.bundle.functions.bundle_functions import add_pipeline_to_bundle

        result = add_pipeline_to_bundle(
            bundle_id=DUMMY_BUNDLE_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
        )

        assert result is True
        mock_link.assert_called_once_with(
            bundle_id=DUMMY_BUNDLE_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
        )

    def test_non_released_pipeline_returns_false(self, mocker, configuration_fixture):
        """Verify function returns False without calling link API when pipeline is not RELEASED."""
        mock_pipeline = MagicMock()
        mock_pipeline.status = "DRAFT"

        mocker.patch(
            "wrapica.bundle.functions.bundle_functions.get_pipeline_obj_from_pipeline_id",
            return_value=mock_pipeline,
        )

        mock_link = mocker.patch(
            "libica.openapi.v3.api.bundle_pipeline_api.BundlePipelineApi.link_pipeline_to_bundle",
        )

        from wrapica.bundle.functions.bundle_functions import add_pipeline_to_bundle

        result = add_pipeline_to_bundle(
            bundle_id=DUMMY_BUNDLE_ID,
            pipeline_id=DUMMY_PIPELINE_ID,
        )

        assert result is False
        mock_link.assert_not_called()


class TestReleaseBundle:
    """Tests for release_bundle."""

    def test_calls_release_bundle_api(self, mocker, configuration_fixture):
        """Verify BundleApi.release_bundle is called with the bundle ID as string."""
        mock_release = mocker.patch(
            "libica.openapi.v3.api.bundle_api.BundleApi.release_bundle",
            return_value=None,
        )

        from wrapica.bundle.functions.bundle_functions import release_bundle

        release_bundle(bundle_id=DUMMY_BUNDLE_ID)

        mock_release.assert_called_once_with(bundle_id=DUMMY_BUNDLE_ID)


class TestDeprecateBundle:
    """Tests for deprecate_bundle."""

    def test_calls_deprecate_bundle_api(self, mocker, configuration_fixture):
        """Verify BundleApi.deprecate_bundle is called with the bundle ID as string."""
        mock_deprecate = mocker.patch(
            "libica.openapi.v3.api.bundle_api.BundleApi.deprecate_bundle",
            return_value=None,
        )

        from wrapica.bundle.functions.bundle_functions import deprecate_bundle

        deprecate_bundle(bundle_id=DUMMY_BUNDLE_ID)

        mock_deprecate.assert_called_once_with(bundle_id=DUMMY_BUNDLE_ID)


class TestListBundlesInProject:
    """Tests for list_bundles_in_project."""

    def test_returns_bundle_list_from_project(self, mocker, configuration_fixture):
        """Verify function returns list of Bundle objects from project bundles response."""
        bundle_a = MagicMock()
        bundle_a.id = DUMMY_BUNDLE_ID
        bundle_b = MagicMock()
        bundle_b.id = "eeeeeeee-5555-4000-8000-eeeeeeeeeeee"

        item_a = MagicMock()
        item_a.bundle = bundle_a
        item_b = MagicMock()
        item_b.bundle = bundle_b

        mock_response = MagicMock()
        mock_response.items = [item_a, item_b]

        mocker.patch(
            "libica.openapi.v3.api.project_api.ProjectApi.get_project_bundles",
            return_value=mock_response,
        )

        from wrapica.bundle.functions.bundle_functions import list_bundles_in_project

        result = list_bundles_in_project(project_id=DUMMY_PROJECT_ID)

        assert result == [bundle_a, bundle_b]
        assert len(result) == 2


class TestCoerceBundleIdOrNameToBundleObj:
    """Tests for coerce_bundle_id_or_name_to_bundle_obj."""

    def test_uuid_delegates_to_bundle_id_lookup(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_bundle_obj_from_bundle_id."""
        mock_bundle = MagicMock()
        mock_bundle.id = DUMMY_BUNDLE_ID

        mock_fn = mocker.patch(
            "wrapica.bundle.functions.bundle_functions.get_bundle_obj_from_bundle_id",
            return_value=mock_bundle,
        )

        from wrapica.bundle.functions.bundle_functions import coerce_bundle_id_or_name_to_bundle_obj

        result = coerce_bundle_id_or_name_to_bundle_obj(DUMMY_BUNDLE_ID)

        mock_fn.assert_called_once_with(DUMMY_BUNDLE_ID)
        assert result == mock_bundle

    def test_non_uuid_delegates_to_bundle_name_lookup(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_bundle_obj_from_bundle_name."""
        mock_bundle = MagicMock()
        mock_bundle.name = "my-bundle"

        mock_fn = mocker.patch(
            "wrapica.bundle.functions.bundle_functions.get_bundle_obj_from_bundle_name",
            return_value=mock_bundle,
        )

        from wrapica.bundle.functions.bundle_functions import coerce_bundle_id_or_name_to_bundle_obj

        result = coerce_bundle_id_or_name_to_bundle_obj("my-bundle")

        mock_fn.assert_called_once_with("my-bundle")
        assert result == mock_bundle
