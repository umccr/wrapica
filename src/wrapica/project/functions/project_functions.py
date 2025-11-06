#!/usr/bin/env python3

"""
Functions for project management
"""

# Standard library imports
from typing import List, Union
from pydantic import UUID4

# Libica imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.project_api import ProjectApi

# Libica models
from libica.openapi.v3.models import Project

# Local imports
from ...utils.configuration import (
    get_icav2_configuration,
    get_project_id_from_env_var,
    get_project_id_from_session_file
)
from ...utils.logger import get_logger
from ...utils.globals import LIBICAV2_DEFAULT_PAGE_SIZE
from ...utils.miscell import is_uuid_format

# Logger helpers
logger = get_logger()

# GLOBALS
PROJECT_MAPPING_DICT = None


def _set_project_mapping_dict():
    global PROJECT_MAPPING_DICT

    PROJECT_MAPPING_DICT = dict(
        map(
            lambda lambda_project_obj: (lambda_project_obj.id, lambda_project_obj.name),
            list_projects()
        )
    )


def _get_project_mapping_dict():
    if PROJECT_MAPPING_DICT is not None:
        return PROJECT_MAPPING_DICT

    _set_project_mapping_dict()

    return _get_project_mapping_dict()


def get_project_obj_from_project_id(
    project_id: Union[UUID4, str]
) -> Project:
    """
    Given a project id return the project object

    :param project_id: The id of the project

    :return: The project object
    :rtype: List[`Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_]

    """

    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectApi(api_client)

    try:
        api_response: Project = api_instance.get_project(str(project_id))
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->get_project_by_id: %s\n" % e)
        raise ApiException

    return api_response


def get_project_obj_from_project_name(
    project_name: str
) -> Project:
    """
    Given a project name, get the project as an object

    Will raise an error if the project id cannot be found

    :param project_name: The name of the project

    :return: The id of the project
    :rtype: str

    :raises ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project import get_project_id_from_project_name

        project_obj = get_project_obj_from_project_name("my_project")

        print(project_obj.id)
        # "1234-5678-9012-3456"

    """
    try:
        return next(
            filter(
                lambda project_obj_iter: project_obj_iter.name == project_name,
                list_projects()
            )
        )
    except StopIteration:
        logger.error(f"Could not find project object from project name {project_name}")
        raise StopIteration


def get_project_id_from_project_name(
    project_name: str
) -> str:
    """
    Given a project name return the id of the project
    Will raise an error if the project id cannot be found

    :param project_name: The name of the project

    :return: The id of the project
    :rtype: str

    :raises StopIteration, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project import get_project_id_from_project_name

        project_id = get_project_id_from_project_name("my_project")

        print(project_id)
        # "1234-5678-9012-3456"
    """
    try:
        return next(filter(
            lambda kv_iter_: kv_iter_[1] == project_name,
            _get_project_mapping_dict().items()
        ))[0]
    except StopIteration as e:
        raise StopIteration("Could not find project id from project name: %s" % project_name) from e


# And vice-versa
def get_project_name_from_project_id(
        project_id: Union[UUID4, str]
) -> str:
    """
    Given a project id, get the project object and return the name attribute
    :param project_id:
    :return:
    """
    try:
        return next(filter(
            lambda kv_iter_: kv_iter_[0] == project_id,
            _get_project_mapping_dict().items()
        ))[1]
    except StopIteration as e:
        raise StopIteration("Could not find project name from project id: %s" % project_id) from e


def check_project_has_data_sharing_enabled(
        project_id: Union[UUID4, str]
) -> bool:
    """
    Given a project id return whether the project has data sharing enabled

    :param project_id: The id of the project

    :return: True if data sharing is enabled, False otherwise
    :rtype: str

    :raises ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project import get_project_id_from_project_name

        project_id = get_project_id_from_project_name("my_project")

        print(check_project_has_data_sharing_enabled(project_id))
        # False
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
        api_response: Project = api_instance.get_project(str(project_id))
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->get_project_by_id: %s\n" % e)
        raise ApiException

    return api_response.data_sharing_enabled


def list_projects(include_hidden_projects: bool = False) -> List[Project]:
    """
    List all projects

    :param include_hidden_projects:

    :return: List of project objects
    :rtype: List[`Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import list_projects
        all_active_projects = list_projects()
    """

    # Create api instance
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectApi(api_client)

    # Set other parameters
    page_size = LIBICAV2_DEFAULT_PAGE_SIZE
    page_offset = 0

    # Initialise project list
    project_list = []

    # example passing only required values which don't have defaults set
    # and optional values
    while True:
        try:
            # Retrieve a list of projects.
            api_response = api_instance.get_projects(
                include_hidden_projects=include_hidden_projects,
                page_size=str(page_size),
                page_offset=str(page_offset)
            )
        except ApiException as e:
            logger.error("Exception when calling ProjectApi->get_projects: %s\n" % e)
            raise ApiException
        project_list.extend(api_response.items)

        # Check page offset and page size against total item count
        if page_offset + page_size >= api_response.total_item_count:
            break
        page_offset += page_size

    return project_list


def coerce_project_id_or_name_to_project_obj(project_id_or_name: str) -> Project:
    """
    Given a project id or name, coerce to a project object

    :param project_id_or_name: The project id or name

    :return: The project object
    """
    # Check if the input is in uuid format
    if is_uuid_format(project_id_or_name):
        return get_project_obj_from_project_id(project_id_or_name)

    return get_project_obj_from_project_name(project_id_or_name)


def coerce_project_id_or_name_to_project_id(project_id_or_name: str) -> str:
    """
    Given a project id or name, return the project id

    :param project_id_or_name: The id or name of the project

    :return: The id of the project
    :rtype: str

    :raises ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project import coerce_project_id_or_name_to_project_id

        project_id = coerce_project_id_or_name_to_project_id("my_project")

        print(project_id)
        # "1234-5678-9012-3456"
    """

    # Check if the input is in uuid format
    if is_uuid_format(project_id_or_name):
        return project_id_or_name

    return get_project_id_from_project_name(project_id_or_name)


def get_project_id() -> str:
    # Try get project id from env var
    try:
        project_id = get_project_id_from_env_var()
    except EnvironmentError:
        logger.debug("Could not get environment variable for project id, trying session file")
    else:
        return project_id

    # Try get project id from session file
    try:
        project_id = get_project_id_from_session_file()
    except KeyError:
        logger.error("Could not get project id from session file")
    else:
        return project_id

    # Could not get project id from either the session
    raise ValueError("Could not get project id from either environment variable or session file")
