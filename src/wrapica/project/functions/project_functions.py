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
    Return the project object for a given project ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string

    :return: The project object matching the given ID
    :rtype: `Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_

    :raises ApiException: If the API call to retrieve the project fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import get_project_obj_from_project_id

        project = get_project_obj_from_project_id("abcd-1234-efab-5678")

        print(f"Project name: {project.name}")
        # Project name: my-project-name
    """

    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectApi(api_client)

    try:
        api_response: Project = api_instance.get_project(project_id=str(project_id))
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->get_project: %s\n" % e)
        raise ApiException

    return api_response


def get_project_obj_from_project_name(
    project_name: str
) -> Project:
    """
    Return the project object matching the given project name.

    :param project_name: The name of the project to look up

    :return: The project object whose name matches the input
    :rtype: `Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_

    :raises StopIteration: If no project matches the given name

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import get_project_obj_from_project_name

        project = get_project_obj_from_project_name("my_project")

        print(f"Project ID: {project.id}")
        # Project ID: abcd1234-ab12-ab12-ab12-abcdef123456
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
    Return the project ID for a given project name.

    :param project_name: The name of the project to look up

    :return: The project ID as a string
    :rtype: str

    :raises StopIteration: If no project matches the given name

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import get_project_id_from_project_name

        project_id = get_project_id_from_project_name("my_project")

        print(f"Project ID: {project_id}")
        # Project ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    try:
        return next(filter(
            lambda kv_iter_: kv_iter_[1] == project_name,
            _get_project_mapping_dict().items()
        ))[0]
    except StopIteration as e:
        raise StopIteration("Could not find project id from project name: %s" % project_name) from e


def get_project_name_from_project_id(
        project_id: Union[UUID4, str]
) -> str:
    """
    Return the project name for a given project ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string

    :return: The project name as a string
    :rtype: str

    :raises StopIteration: If no project matches the given ID

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import get_project_name_from_project_id

        project_name = get_project_name_from_project_id("abcd-1234-efab-5678")

        print(f"Project name: {project_name}")
        # Project name: my-project-name
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
    Check whether a project has data sharing enabled.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string

    :return: True if data sharing is enabled, False otherwise
    :rtype: bool

    :raises ApiException: If the API call to retrieve the project fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import check_project_has_data_sharing_enabled

        is_enabled = check_project_has_data_sharing_enabled("abcd-1234-efab-5678")

        print(f"Data sharing enabled: {is_enabled}")
        # Data sharing enabled: True
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
        api_response: Project = api_instance.get_project(project_id=str(project_id))
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->get_project: %s\n" % e)
        raise ApiException

    return api_response.data_sharing_enabled


def list_projects(include_hidden_projects: bool = False) -> List[Project]:
    """
    Return a list of all projects accessible to the user.

    :param include_hidden_projects: Whether to include hidden projects in the result.
        Defaults to False, in which case only visible projects are returned

    :return: The list of project objects accessible to the user
    :rtype: List[`Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_]

    :raises ApiException: If the API call to retrieve projects fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import list_projects

        projects = list_projects()

        print(f"Found {len(projects)} project(s)")
        # Found 3 project(s)
        for project in projects:
            print(f"Project ID: {project.id}, Name: {project.name}")
            # Project ID: abcd1234-..., Name: my-project-name
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
    Coerce a project ID or name to a project object.

    :param project_id_or_name: The project identifier as a UUID-formatted string or project name

    :return: The project object resolved from the input
    :rtype: `Project <https://umccr.github.io/libica/openapi/v3/docs/Project/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import coerce_project_id_or_name_to_project_obj

        project = coerce_project_id_or_name_to_project_obj("my_project")

        print(f"Project ID: {project.id}, Name: {project.name}")
        # Project ID: abcd1234-ab12-ab12-ab12-abcdef123456, Name: my-project-name
    """
    # Check if the input is in uuid format
    if is_uuid_format(project_id_or_name):
        return get_project_obj_from_project_id(project_id_or_name)

    return get_project_obj_from_project_name(project_id_or_name)


def coerce_project_id_or_name_to_project_id(project_id_or_name: str) -> str:
    """
    Coerce a project ID or name to a project ID string.

    :param project_id_or_name: The project identifier as a UUID-formatted string or project name

    :return: The project ID as a string
    :rtype: str

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import coerce_project_id_or_name_to_project_id

        project_id = coerce_project_id_or_name_to_project_id("my_project")

        print(f"Project ID: {project_id}")
        # Project ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """

    # Check if the input is in uuid format
    if is_uuid_format(project_id_or_name):
        return project_id_or_name

    return get_project_id_from_project_name(project_id_or_name)


def get_project_id() -> str:
    """
    Return the active project ID from environment variable or session file.

    :return: The project ID as a string
    :rtype: str

    :raises ValueError: If the project ID cannot be found in environment or session

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project import get_project_id

        project_id = get_project_id()

        print(f"Active project ID: {project_id}")
        # Active project ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
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
