"""Shared test fixtures for wrapica test suite."""
import pytest
from unittest.mock import MagicMock

from tests.test_helpers import DUMMY_ACCESS_TOKEN


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set dummy environment variables to prevent real credential usage."""
    monkeypatch.setenv("ICAV2_ACCESS_TOKEN", DUMMY_ACCESS_TOKEN)
    monkeypatch.setenv("ICAV2_BASE_URL", "https://192.0.2.1/ica/rest")  # RFC 5737 non-routable
    monkeypatch.setenv("ICAV2_PROJECT_ID", "00000000-0000-4000-8000-000000000000")


@pytest.fixture
def configuration_fixture(mocker):
    """Return a mocked libica Configuration object and patch get_icav2_configuration."""
    mock_config = MagicMock()
    mock_config.host = "https://192.0.2.1/ica/rest"
    mock_config.access_token = DUMMY_ACCESS_TOKEN
    mocker.patch(
        "wrapica.utils.configuration.get_icav2_configuration",
        return_value=mock_config
    )
    return mock_config


@pytest.fixture(autouse=True)
def api_client_mock(mocker):
    """Patch ApiClient to prevent real HTTP connections."""
    mock_client = MagicMock()
    mocker.patch("libica.openapi.v3.ApiClient.__init__", return_value=None)
    mocker.patch("libica.openapi.v3.ApiClient.__enter__", return_value=mock_client)
    mocker.patch("libica.openapi.v3.ApiClient.__exit__", return_value=False)
    return mock_client
