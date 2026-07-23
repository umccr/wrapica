"""Tests for wrapica.utils.configuration module."""

import time
from unittest.mock import patch, MagicMock

import pytest

import wrapica.utils.configuration as config_module
from wrapica.utils.configuration import (
    get_icav2_configuration,
    get_icav2_access_token,
    check_access_token_expiry,
    get_project_id_from_env_var,
    get_jwt_token_obj,
)
from tests.test_helpers import DUMMY_ACCESS_TOKEN


class TestGetIcav2Configuration:
    """Tests for get_icav2_configuration."""

    def test_returns_configuration_with_correct_host_and_token(self, monkeypatch, mocker):
        """Reset global to None and verify returned Configuration has correct host and access_token."""
        # Reset the global singleton to force re-initialization
        monkeypatch.setattr(config_module, "ICAV2_CONFIGURATION", None)

        # Mock check_access_token_expiry so it doesn't try to decode the dummy token
        mocker.patch(
            "wrapica.utils.configuration.check_access_token_expiry",
            return_value=True,
        )

        configuration = get_icav2_configuration()

        assert configuration.host == "https://192.0.2.1/ica/rest"
        assert configuration.access_token == DUMMY_ACCESS_TOKEN


class TestGetIcav2AccessToken:
    """Tests for get_icav2_access_token."""

    def test_returns_token_from_env_var(self, mocker):
        """With env var set and token not expired, returns the env var token."""
        mocker.patch(
            "wrapica.utils.configuration.check_access_token_expiry",
            return_value=True,
        )

        token = get_icav2_access_token()

        assert token == DUMMY_ACCESS_TOKEN

    def test_raises_not_implemented_without_env_var(self, monkeypatch, mocker):
        """Without ICAV2_ACCESS_TOKEN env var, falls back to session file and raises NotImplementedError."""
        monkeypatch.delenv("ICAV2_ACCESS_TOKEN")

        with pytest.raises(NotImplementedError):
            get_icav2_access_token()


class TestCheckAccessTokenExpiry:
    """Tests for check_access_token_expiry."""

    def test_returns_true_for_future_exp(self, mocker):
        """Token with exp in the future returns True (not expired)."""
        future_exp = int(time.time()) + 3600  # 1 hour from now
        mocker.patch(
            "wrapica.utils.configuration.get_jwt_token_obj",
            return_value={"exp": future_exp},
        )

        result = check_access_token_expiry("some.jwt.token")

        assert result is True

    def test_returns_false_for_past_exp(self, mocker):
        """Token with exp in the past returns False (expired)."""
        past_exp = int(time.time()) - 3600  # 1 hour ago
        mocker.patch(
            "wrapica.utils.configuration.get_jwt_token_obj",
            return_value={"exp": past_exp},
        )

        result = check_access_token_expiry("some.jwt.token")

        assert result is False


class TestGetProjectIdFromEnvVar:
    """Tests for get_project_id_from_env_var."""

    def test_returns_value_when_env_var_set(self):
        """With ICAV2_PROJECT_ID set, returns its value."""
        project_id = get_project_id_from_env_var()

        assert project_id == "00000000-0000-4000-8000-000000000000"

    def test_raises_environment_error_without_env_var(self, monkeypatch):
        """Without ICAV2_PROJECT_ID, raises EnvironmentError."""
        monkeypatch.delenv("ICAV2_PROJECT_ID")

        with pytest.raises(EnvironmentError):
            get_project_id_from_env_var()


class TestGetJwtTokenObj:
    """Tests for get_jwt_token_obj."""

    def test_decodes_jwt_token(self, mocker):
        """With mocked jwt.decode, returns the decoded token object."""
        expected_payload = {"sub": "user123", "exp": 9999999999, "aud": "ica"}
        mocker.patch(
            "wrapica.utils.configuration.decode",
            return_value=expected_payload,
        )

        result = get_jwt_token_obj("fake.jwt.token", "ica")

        assert result == expected_payload
        # Verify decode was called with correct options
        from wrapica.utils.configuration import decode as mocked_decode

        mocked_decode.assert_called_once_with(
            "fake.jwt.token",
            options={"verify_signature": False, "require_exp": True},
            audience=["ica"],
            algorithms="RS256",
        )
