#!/usr/bin/env python3

# Standard imports
from time import sleep
from typing import cast

# Libica API imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.job_api import JobApi

# Libica model imports
from libica.openapi.v3.models import Job

# Util imports
from ...literals import JobStatusType
from ...utils.configuration import get_icav2_configuration


def get_job(
    job_id: str
) -> Job:
    """
    Get a job (such as a copy job)

    :param job_id:

    :return: The job object
    :rtype: `Job <https://umccr-illumina.github.io/libica/openapi/v2/docs/Job/>`_

    """

    configuration = get_icav2_configuration()

    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = JobApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a job.
        api_response: Job = api_instance.get_job(job_id)
    except ApiException as e:
        raise ApiException("Exception when calling JobApi->get_job: %s\n" % e)

    return api_response


def wait_for_job_completion(
        job_id: str,
        raise_on_failure: bool = True
) -> JobStatusType:

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

