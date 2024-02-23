#!/usr/bin/env python3

"""
Functions for project management
"""

# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.project_api import ProjectApi
from libica.openapi.v2.model.project_paged_list import ProjectPagedList

# Local imports
from ...utils.configuration import get_icav2_configuration
from ...utils.logger import get_logger
from ...utils.globals import LIBICAV2_DEFAULT_PAGE_SIZE

# Logger helpers
logger = get_logger()


def get_project_id_from_project_name(
    project_name: str
) -> str:
    """
    Given a project name return the id of the project
    Will raise an error if the project id cannot be found
    :param project_name:
    :return:
    """

    # Configuration
    configuration = get_icav2_configuration()

    # Collect the projects
    # We assume that there aren't more than 1000 projects anyway
    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        api_instance = ProjectApi(api_client)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Retrieve a list of projects.
        api_response: ProjectPagedList = api_instance.get_projects(
            page_size=LIBICAV2_DEFAULT_PAGE_SIZE,
         )
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->get_projects: %s\n" % e)
        raise ApiException

    try:
        return next(
            filter(
                lambda project_iter: project_iter.name == project_name,
                api_response.items
            )
        )
    except StopIteration:
        raise ValueError(f"Project with name {project_name} not found")

