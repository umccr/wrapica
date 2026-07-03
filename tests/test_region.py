"""Tests for wrapica.region functions."""
import pytest
from unittest.mock import MagicMock

from tests.test_helpers import DUMMY_REGION_ID, DUMMY_PROJECT_ID, DUMMY_BUNDLE_ID


class TestGetRegions:
    """Tests for get_regions."""

    def test_returns_region_items(self, mocker, configuration_fixture):
        """Verify get_regions returns items from RegionApi.get_regions response."""
        region_a = MagicMock()
        region_a.id = DUMMY_REGION_ID
        region_a.city_name = "Sydney"

        region_b = MagicMock()
        region_b.id = "cccccccc-3333-4000-8000-cccccccccccc"
        region_b.city_name = "Tokyo"

        mock_response = MagicMock()
        mock_response.items = [region_a, region_b]

        mocker.patch(
            "libica.openapi.v3.api.region_api.RegionApi.get_regions",
            return_value=mock_response,
        )

        from wrapica.region.functions.region_functions import get_regions

        result = get_regions()

        assert result == [region_a, region_b]
        assert len(result) == 2


class TestGetRegionObjFromRegionId:
    """Tests for get_region_obj_from_region_id."""

    def test_returns_region_matching_id(self, mocker, configuration_fixture):
        """Verify the Region object returned has an id matching the request."""
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID
        mock_region.city_name = "Sydney"

        mocker.patch(
            "libica.openapi.v3.api.region_api.RegionApi.get_region",
            return_value=mock_region,
        )

        from wrapica.region.functions.region_functions import get_region_obj_from_region_id

        result = get_region_obj_from_region_id(DUMMY_REGION_ID)

        assert result.id == DUMMY_REGION_ID


class TestGetRegionObjFromCityName:
    """Tests for get_region_obj_from_city_name."""

    def test_matching_city_name(self, mocker, configuration_fixture):
        """Verify correct region is returned when city name matches."""
        region_sydney = MagicMock()
        region_sydney.city_name = "Sydney"
        region_sydney.id = DUMMY_REGION_ID

        region_tokyo = MagicMock()
        region_tokyo.city_name = "Tokyo"
        region_tokyo.id = "cccccccc-3333-4000-8000-cccccccccccc"

        mocker.patch(
            "wrapica.region.functions.region_functions.get_regions",
            return_value=[region_sydney, region_tokyo],
        )

        from wrapica.region.functions.region_functions import get_region_obj_from_city_name

        result = get_region_obj_from_city_name("Sydney")

        assert result.city_name == "Sydney"
        assert result.id == DUMMY_REGION_ID

    def test_non_matching_city_name_raises_stop_iteration(self, mocker, configuration_fixture):
        """Verify StopIteration is raised when city name not found."""
        region_sydney = MagicMock()
        region_sydney.city_name = "Sydney"

        mocker.patch(
            "wrapica.region.functions.region_functions.get_regions",
            return_value=[region_sydney],
        )

        from wrapica.region.functions.region_functions import get_region_obj_from_city_name

        with pytest.raises(StopIteration):
            get_region_obj_from_city_name("NonExistentCity")


class TestCoerceRegionIdOrCityNameToRegionObj:
    """Tests for coerce_region_id_or_city_name_to_region_obj."""

    def test_uuid_delegates_to_region_id_lookup(self, mocker, configuration_fixture):
        """Verify UUID input delegates to get_region_obj_from_region_id."""
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID

        mock_fn = mocker.patch(
            "wrapica.region.functions.region_functions.get_region_obj_from_region_id",
            return_value=mock_region,
        )

        from wrapica.region.functions.region_functions import coerce_region_id_or_city_name_to_region_obj

        result = coerce_region_id_or_city_name_to_region_obj(DUMMY_REGION_ID)

        mock_fn.assert_called_once_with(DUMMY_REGION_ID)
        assert result == mock_region

    def test_non_uuid_delegates_to_city_name_lookup(self, mocker, configuration_fixture):
        """Verify non-UUID input delegates to get_region_obj_from_city_name."""
        mock_region = MagicMock()
        mock_region.city_name = "Sydney"

        mock_fn = mocker.patch(
            "wrapica.region.functions.region_functions.get_region_obj_from_city_name",
            return_value=mock_region,
        )

        from wrapica.region.functions.region_functions import coerce_region_id_or_city_name_to_region_obj

        result = coerce_region_id_or_city_name_to_region_obj("Sydney")

        mock_fn.assert_called_once_with("Sydney")
        assert result == mock_region


class TestGetRegionObjFromProjectId:
    """Tests for get_region_obj_from_project_id."""

    def test_returns_region_from_project(self, mocker, configuration_fixture):
        """Verify get_region_obj_from_project_id returns the project's region attribute."""
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID
        mock_region.city_name = "Sydney"

        mock_project = MagicMock()
        mock_project.region = mock_region

        mocker.patch(
            "wrapica.project.get_project_obj_from_project_id",
            return_value=mock_project,
        )

        from wrapica.region.functions.region_functions import get_region_obj_from_project_id

        result = get_region_obj_from_project_id(DUMMY_PROJECT_ID)

        assert result == mock_region
        assert result.id == DUMMY_REGION_ID


class TestGetRegionFromBundleId:
    """Tests for get_region_from_bundle_id."""

    def test_returns_region_from_bundle(self, mocker, configuration_fixture):
        """Verify get_region_from_bundle_id returns the bundle's region attribute."""
        mock_region = MagicMock()
        mock_region.id = DUMMY_REGION_ID
        mock_region.city_name = "Sydney"

        mock_bundle = MagicMock()
        mock_bundle.region = mock_region

        mocker.patch(
            "wrapica.bundle.get_bundle_obj_from_bundle_id",
            return_value=mock_bundle,
        )

        from wrapica.region.functions.region_functions import get_region_from_bundle_id

        result = get_region_from_bundle_id(DUMMY_BUNDLE_ID)

        assert result == mock_region
        assert result.id == DUMMY_REGION_ID
