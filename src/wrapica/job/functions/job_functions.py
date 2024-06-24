#!/usr/bin/env python3

# Libica API imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.job_api import JobApi

# Libica model imports
from libica.openapi.v2.models import Job

# Util imports
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
