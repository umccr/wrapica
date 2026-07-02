#!/usr/bin/env python3

# Standard imports
from time import sleep
from typing import cast, Union

# Libica API imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.job_api import JobApi

# Libica model imports
from libica.openapi.v3.models import Job
from pydantic import UUID4

# Util imports
from ...literals import JobStatusType
from ...utils.configuration import get_icav2_configuration
from ...utils.miscell import coerce_to_uuid4_obj


def get_job(
    job_id: Union[UUID4, str]
) -> Job:
    """
    Retrieve a job object by its identifier.

    :param job_id: The job identifier as a UUID4 object or UUID-formatted string

    :return: The job object matching the given ID
    :rtype: `Job <https://umccr.github.io/libica/openapi/v3/docs/Job/>`_

    :raises ApiException: If the API call to retrieve the job fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.job import get_job

        job = get_job(job_id="abcd1234-ab12-ab12-ab12-abcdef123456")

        print(f"Job status: {job.status}")
        # Job status: RUNNING
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = JobApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a job.
        api_response: Job = api_instance.get_job(job_id=coerce_to_uuid4_obj(job_id))
    except ApiException as e:
        raise ApiException("Exception when calling JobApi->get_job: %s\n" % e)

    return api_response


def wait_for_job_completion(
        job_id: Union[UUID4, str],
        raise_on_failure: bool = True
) -> JobStatusType:
    """
    Poll a job until it reaches a terminal status.

    :param job_id: The job identifier as a UUID4 object or UUID-formatted string
    :param raise_on_failure: Whether to raise an exception if the job fails.
        Defaults to True, in which case an exception is raised on failure

    :return: The terminal status of the job
    :rtype: str

    :raises Exception: If the job ends with a non-success status and raise_on_failure is True
    :raises ApiException: If the API call to retrieve the job fails during polling

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.job import wait_for_job_completion

        status = wait_for_job_completion(
            job_id="abcd1234-ab12-ab12-ab12-abcdef123456"
        )

        print(f"Job completed with status: {status}")
        # Job completed with status: SUCCEEDED
    """
    while True:
        # Get the job objects
        job_obj = get_job(job_id)

        # Get the job status
        job_status = cast(JobStatusType, job_obj.status)

        if job_status in ['SUCCEEDED']:
            return job_status
        elif job_status in ['FAILED', 'PARTIALLY_SUCCEEDED', 'STOPPED']:
            if raise_on_failure:
                raise Exception(f"Job {job_id} failed with status {job_status}")
            else:
                return job_status

        sleep(5)

