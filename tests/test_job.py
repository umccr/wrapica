"""Tests for wrapica.job module (get_job, wait_for_job_completion)."""
import pytest
from unittest.mock import MagicMock, patch

from tests.test_helpers import DUMMY_JOB_ID


class TestGetJob:
    """Tests for get_job function."""

    def test_get_job_returns_job_object(self, mocker):
        """
        WHEN get_job is called with a valid job_id,
        THEN it returns the Job object from the mocked JobApi.get_job.

        Validates: Requirements 9.1
        """
        mock_job = MagicMock()
        mock_job.id = DUMMY_JOB_ID
        mock_job.status = "RUNNING"

        # Mock the configuration
        mocker.patch(
            "wrapica.job.functions.job_functions.get_icav2_configuration",
            return_value=MagicMock()
        )

        # Mock JobApi.get_job
        mock_get_job = mocker.patch(
            "wrapica.job.functions.job_functions.JobApi.get_job",
            return_value=mock_job
        )

        from wrapica.job.functions.job_functions import get_job

        result = get_job(job_id=DUMMY_JOB_ID)

        assert result is mock_job
        mock_get_job.assert_called_once()


class TestWaitForJobCompletion:
    """Tests for wait_for_job_completion function."""

    def test_succeeded_returns_immediately(self, mocker):
        """
        WHEN wait_for_job_completion is called and get_job returns status SUCCEEDED,
        THEN it returns "SUCCEEDED" without sleeping.

        Validates: Requirements 9.2
        """
        mock_job = MagicMock()
        mock_job.status = "SUCCEEDED"

        mocker.patch(
            "wrapica.job.functions.job_functions.get_job",
            return_value=mock_job
        )
        mock_sleep = mocker.patch(
            "wrapica.job.functions.job_functions.sleep"
        )

        from wrapica.job.functions.job_functions import wait_for_job_completion

        result = wait_for_job_completion(job_id=DUMMY_JOB_ID)

        assert result == "SUCCEEDED"
        mock_sleep.assert_not_called()

    def test_failed_with_raise_on_failure_raises_exception(self, mocker):
        """
        WHEN wait_for_job_completion is called with raise_on_failure=True
        and get_job returns status FAILED,
        THEN it raises an Exception.

        Validates: Requirements 9.3
        """
        mock_job = MagicMock()
        mock_job.status = "FAILED"

        mocker.patch(
            "wrapica.job.functions.job_functions.get_job",
            return_value=mock_job
        )
        mock_sleep = mocker.patch(
            "wrapica.job.functions.job_functions.sleep"
        )

        from wrapica.job.functions.job_functions import wait_for_job_completion

        with pytest.raises(Exception, match="failed with status FAILED"):
            wait_for_job_completion(job_id=DUMMY_JOB_ID, raise_on_failure=True)

        mock_sleep.assert_not_called()

    def test_failed_with_raise_on_failure_false_returns_status(self, mocker):
        """
        WHEN wait_for_job_completion is called with raise_on_failure=False
        and get_job returns status FAILED,
        THEN it returns "FAILED" without raising.

        Validates: Requirements 9.4
        """
        mock_job = MagicMock()
        mock_job.status = "FAILED"

        mocker.patch(
            "wrapica.job.functions.job_functions.get_job",
            return_value=mock_job
        )
        mock_sleep = mocker.patch(
            "wrapica.job.functions.job_functions.sleep"
        )

        from wrapica.job.functions.job_functions import wait_for_job_completion

        result = wait_for_job_completion(job_id=DUMMY_JOB_ID, raise_on_failure=False)

        assert result == "FAILED"
        mock_sleep.assert_not_called()
