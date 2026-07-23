#!/usr/bin/env python

"""
Functions relating to the 'data' endpoint
"""

# Standard imports
from pathlib import Path
from typing import Optional, Union

# Libica api imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.data_api import DataApi
from urllib.parse import urlunparse, urlparse

# Libica model imports
from libica.openapi.v3.models import (
    Data,
    ProjectData
)
from pydantic import UUID4

# Local imports
from ...project_data import is_data_id_format
from ...utils.configuration import get_icav2_configuration
from ...utils.logger import get_logger
from ...utils.globals import (
    FOLDER_DATA_TYPE,
    FILE_DATA_TYPE,
    ICAV2_URI_SCHEME
)

# Log imports
logger = get_logger()


def get_data_obj_from_data_id(
        data_id: Union[UUID4, str],
        region_id: Optional[Union[UUID4, str]] = None
) -> Data:
    """
    Return the data object for a given data ID.

    :param data_id: The data identifier as a UUID4 object or UUID-formatted string
    :param region_id: The region identifier to scope the lookup. Defaults to None,
        in which case the default region is used

    :return: The data object matching the given ID
    :rtype: `Data <https://umccr.github.io/libica/openapi/v3/docs/Data/>`_

    :raises ApiException: If the API call to retrieve the data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import get_data_obj_from_data_id

        data_obj = get_data_obj_from_data_id("fil.123456")

        print(f"ID: {data_obj.id}, Name: {data_obj.details.name}")
        # ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    from ...region.functions.region_functions import get_default_region
    if region_id is None:
        region_id = get_default_region().id

    # Get the data urn
    data_urn = f"urn:ilmn:ica:region:{region_id}:data:{data_id}"

    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = DataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a data.
        api_response: Data = api_instance.get_data(data_urn=data_urn)
    except ApiException as e:
        logger.error("Exception when calling DataApi->get_data: %s\n" % e)
        raise ApiException

    return api_response


def get_owning_project_id(
        data_id: Union[UUID4, str],
        region_id: Optional[Union[UUID4, str]] = None
) -> str:
    """
    Return the owning project ID for a given data object.

    :param data_id: The data identifier as a UUID4 object or UUID-formatted string
    :param region_id: The region identifier to scope the lookup. Defaults to None,
        in which case the default region is used

    :return: The owning project ID as a string
    :rtype: str

    :raises ApiException: If the API call to retrieve the data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import get_owning_project_id

        owning_project_id = get_owning_project_id("fil.123456")

        print(owning_project_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """
    data_obj = get_data_obj_from_data_id(data_id, region_id)
    return str(data_obj.details.owning_project_id)


def get_project_data_obj_from_data_id(
        data_id: Union[UUID4, str]
) -> ProjectData:
    """
    Return the project data object for a given data ID.

    :param data_id: The data identifier as a UUID4 object or UUID-formatted string

    :return: The project data object matching the given ID
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ApiException: If the API call to retrieve the data or project data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import get_project_data_obj_from_data_id

        project_data_obj = get_project_data_obj_from_data_id("fil.123456")

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    # Local imports to prevent circular dependency
    from ...project_data import get_project_data_obj_by_id

    # Get the owning project id
    project_id = get_owning_project_id(data_id)

    return get_project_data_obj_by_id(project_id, data_id)


def convert_icav2_uri_to_data_obj(
        data_uri: str,
        create_data_if_not_found: bool = False
) -> Data:
    """
    Convert an icav2:// URI to a data object.

    :param data_uri: The icav2:// URI to convert to a data object
    :param create_data_if_not_found: Create the data entry if it does not exist.
        Defaults to False

    :return: The data object resolved from the URI
    :rtype: `Data <https://umccr.github.io/libica/openapi/v3/docs/Data/>`_

    :raises ApiException: If the API call to retrieve or create the data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import convert_icav2_uri_to_data_obj

        data_obj = convert_icav2_uri_to_data_obj(
            "icav2://project_id/path/to/file.txt"
        )

        print(f"ID: {data_obj.id}, Name: {data_obj.details.name}")
        # ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    # Local imports to prevent circular dependency
    from ...project_data import convert_icav2_uri_to_project_data_obj

    # Convert to project data object
    project_data_obj = convert_icav2_uri_to_project_data_obj(
        data_uri=data_uri,
        create_data_if_not_found=create_data_if_not_found
    )

    # Return as data object
    return project_data_obj.data


def convert_data_obj_to_icav2_uri(data_obj: Data) -> str:
    """
    Convert a data object to an icav2:// URI string.

    :param data_obj: The data object to convert to an icav2:// URI

    :return: The icav2:// URI representation of the data object
    :rtype: str

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import (
            get_data_obj_from_data_id,
            convert_data_obj_to_icav2_uri
        )

        data_obj = get_data_obj_from_data_id("fil.123456")
        data_uri = convert_data_obj_to_icav2_uri(data_obj)

        print(data_uri)
        # icav2://abcd1234-ab12-ab12-ab12-abcdef123456/path/to/file.txt
    """
    return str(
        urlunparse(
            (
                ICAV2_URI_SCHEME,
                str(data_obj.details.owning_project_id),
                data_obj.details.path,
                None, None, None
            )
        )
    )


def coerce_data_id_path_or_icav2_uri_to_data_obj(
        data_id_path_or_uri: str,
        create_data_if_not_found: bool = False
) -> Optional[Data]:
    """
    Coerce a data ID, file path, or icav2:// URI to a data object.

    :param data_id_path_or_uri: The data identifier, absolute path, or icav2:// URI to resolve
    :param create_data_if_not_found: Create the data entry if it does not exist.
        Defaults to False. Only applicable to paths or URIs

    :return: The resolved data object, or None if the path is the root directory
    :rtype: Optional[`Data <https://umccr.github.io/libica/openapi/v3/docs/Data/>`_]

    :raises ApiException: If the API call to retrieve or create the data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.data import coerce_data_id_path_or_icav2_uri_to_data_obj

        data_obj = coerce_data_id_path_or_icav2_uri_to_data_obj("fil.123456")

        print(f"ID: {data_obj.id}, Name: {data_obj.details.name}")
        # ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt

    .. code-block:: python
        :linenos:

        from wrapica.data import coerce_data_id_path_or_icav2_uri_to_data_obj

        data_obj = coerce_data_id_path_or_icav2_uri_to_data_obj(
            "icav2://project-id/path/to/file.txt"
        )

        print(f"ID: {data_obj.id}, Name: {data_obj.details.name}")
        # ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    from ...project_data import get_project_data_obj_from_project_id_and_path
    from ...project import get_project_id

    if is_data_id_format(data_id_path_or_uri):
        # Data ID, easy to convert across
        return get_data_obj_from_data_id(
            data_id=data_id_path_or_uri
        )
    elif urlparse(data_id_path_or_uri).scheme == ICAV2_URI_SCHEME:
        # ICAv2 URI, convert to data object
        return convert_icav2_uri_to_data_obj(
            data_uri=data_id_path_or_uri,
            create_data_if_not_found=create_data_if_not_found
        )
    else:
        # Data Path, convert to data object
        # Not as straight forward, need to first find this data, then convert to data object
        project_id = get_project_id()
        if Path(data_id_path_or_uri) == Path("/"):
            # There is no data id for the root directory (nor can we create one), we return none
            return None

        project_data_obj = get_project_data_obj_from_project_id_and_path(
            project_id=project_id,
            data_path=Path(data_id_path_or_uri),
            data_type=FOLDER_DATA_TYPE if data_id_path_or_uri.endswith("/") else FILE_DATA_TYPE,
            create_data_if_not_found=create_data_if_not_found
        )
        return project_data_obj.data
