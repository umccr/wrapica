"""Tests for wrapica.user.functions.user_functions module."""

from unittest.mock import MagicMock, patch

import pytest

from tests.test_helpers import DUMMY_USER_ID

from wrapica.user.functions.user_functions import (
    get_user_obj_from_user_id,
    get_user_obj_from_user_name,
    get_user_id_from_configuration,
    coerce_user_id_or_name_to_user_obj,
    get_tenant_id_for_user,
)


@pytest.fixture
def mock_user():
    """Create a mock User object with standard attributes."""
    user = MagicMock()
    user.id = DUMMY_USER_ID
    user.firstname = "Jane"
    user.lastname = "Doe"
    user.username = "jdoe"
    user.tenant_id = "tttttttt-0000-4000-8000-tttttttttttt"
    return user


class TestGetUserObjFromUserId:
    """Tests for get_user_obj_from_user_id."""

    def test_returns_user_from_api(self, mocker, mock_user):
        """Calls UserApi.get_user with the given ID and returns the user object."""
        mocker.patch(
            "wrapica.user.functions.user_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "libica.openapi.v3.api.user_api.UserApi.get_user",
            return_value=mock_user,
        )

        result = get_user_obj_from_user_id(DUMMY_USER_ID)

        assert result == mock_user
        assert result.id == DUMMY_USER_ID


class TestGetUserObjFromUserName:
    """Tests for get_user_obj_from_user_name."""

    def test_returns_matching_user(self, mocker, mock_user):
        """Returns user whose firstname + ' ' + lastname matches the given name."""
        user_list = MagicMock()
        user_list.items = [mock_user]

        mocker.patch(
            "wrapica.user.functions.user_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "libica.openapi.v3.api.user_api.UserApi.get_users",
            return_value=user_list,
        )

        result = get_user_obj_from_user_name("Jane Doe")

        assert result == mock_user

    def test_raises_value_error_when_no_match(self, mocker):
        """Raises ValueError when no user matches the given name."""
        other_user = MagicMock()
        other_user.firstname = "John"
        other_user.lastname = "Smith"

        user_list = MagicMock()
        user_list.items = [other_user]

        mocker.patch(
            "wrapica.user.functions.user_functions.get_icav2_configuration",
            return_value=MagicMock(),
        )
        mocker.patch(
            "libica.openapi.v3.api.user_api.UserApi.get_users",
            return_value=user_list,
        )

        with pytest.raises(ValueError):
            get_user_obj_from_user_name("Jane Doe")


class TestGetUserIdFromConfiguration:
    """Tests for get_user_id_from_configuration."""

    def test_returns_sub_from_jwt(self, mocker):
        """Extracts the 'sub' claim from the JWT token via get_jwt_token_obj."""
        mock_config = MagicMock()
        mock_config.access_token = "fake.jwt.token"

        mocker.patch(
            "wrapica.user.functions.user_functions.get_icav2_configuration",
            return_value=mock_config,
        )
        mocker.patch(
            "wrapica.user.functions.user_functions.get_jwt_token_obj",
            return_value={"sub": DUMMY_USER_ID},
        )

        result = get_user_id_from_configuration()

        assert result == DUMMY_USER_ID


class TestCoerceUserIdOrNameToUserObj:
    """Tests for coerce_user_id_or_name_to_user_obj."""

    def test_uuid_delegates_to_id_lookup(self, mocker, mock_user):
        """When input is a valid UUID, delegates to get_user_obj_from_user_id."""
        mocker.patch(
            "wrapica.user.functions.user_functions.get_user_obj_from_user_id",
            return_value=mock_user,
        )

        result = coerce_user_id_or_name_to_user_obj(DUMMY_USER_ID)

        assert result == mock_user
        from wrapica.user.functions.user_functions import get_user_obj_from_user_id as patched
        patched.assert_called_once_with(DUMMY_USER_ID)

    def test_non_uuid_delegates_to_name_lookup(self, mocker, mock_user):
        """When input is not a valid UUID, delegates to get_user_obj_from_user_name."""
        mocker.patch(
            "wrapica.user.functions.user_functions.get_user_obj_from_user_name",
            return_value=mock_user,
        )

        result = coerce_user_id_or_name_to_user_obj("Jane Doe")

        assert result == mock_user
        from wrapica.user.functions.user_functions import get_user_obj_from_user_name as patched
        patched.assert_called_once_with("Jane Doe")


class TestGetTenantIdForUser:
    """Tests for get_tenant_id_for_user."""

    def test_returns_tenant_id_from_current_user(self, mocker, mock_user):
        """Gets user ID from configuration, then returns the user's tenant_id."""
        mocker.patch(
            "wrapica.user.functions.user_functions.get_user_id_from_configuration",
            return_value=DUMMY_USER_ID,
        )
        mocker.patch(
            "wrapica.user.functions.user_functions.get_user_obj_from_user_id",
            return_value=mock_user,
        )

        result = get_tenant_id_for_user()

        assert result == "tttttttt-0000-4000-8000-tttttttttttt"
