#!/usr/bin/env python

"""
Functions relating to the 'data' endpoint
"""

# Standard imports
from pathlib import Path
from typing import Optional

# Libica api imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.data_api import DataApi
from urllib.parse import urlunparse, urlparse

# Libica model imports
from libica.openapi.v3.models import (
    Data,
    ProjectData
)


# Local imports
from ...project_data import is_data_id_format
from ...utils.configuration import get_icav2_configuration
from ...utils.logger import get_logger
from ...utils.globals import (
    FOLDER_DATA_TYPE,
    FILE_DATA_TYPE,
    ICAV2_URI_SCHEME
)

logger = get_logger()


def get_data_obj_from_data_id(data_id: str, region_id: Optional[str] = None) -> Data:
    """
    Get data object by id

    :param data_id:
    :param region_id:

    :return: The data object
    :rtype: `Data <https://umccr-illumina.github.io/libica/openapi/v2/docs/Data/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import get_data_obj_by_id

        # Set vars
        data_id = "fil.123456"

        # Get data object by id
        data_obj = get_data_obj_by_id(data_id)
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
        api_response: Data = api_instance.get_data(data_urn)
    except ApiException as e:
        logger.error("Exception when calling DataApi->get_data: %s\n" % e)
        raise ApiException

    return api_response


def get_owning_project_id(data_id: str, region_id: Optional[str] = None) -> str:
    """
    Get the owning project id of a data object

    :param data_id:
    :param region_id:

    :return: The owning project id
    :rtype: str

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import get_owning_project_id

        # Set vars
        data_id = "fil.123456"

        # Get owning project id
        owning_project_id = get_owning_project_id(data_id)

    """
    data_obj = get_data_obj_from_data_id(data_id, region_id)
    return data_obj.details.owning_project_id


def get_project_data_obj_from_data_id(data_id: str) -> ProjectData:
    """
    Get the project data object from a data id

    :param data_id:  The data id

    :return: The project data object
    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import get_project_data_obj_from_data_id

        # Set vars
        data_id = "fil.123456"

        # Get project data object from just data id
        project_data_obj = get_project_data_obj_from_data_id(data_id)
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
    Given a data uri, convert to a data object

    :param data_uri:  The data uri
    :param create_data_if_not_found:  If true, create the data if not found

    :return: The data object
    :rtype: `Data <https://umccr-illumina.github.io/libica/openapi/v2/docs/Data/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import convert_icav2_uri_to_data_obj

        # Set vars
        data_uri = "icav2://project_id/path/to/data_obj"

        # Convert to data object
        data_obj = convert_icav2_uri_to_data_obj(data_uri)
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
    Given a data object, convert to a data uri

    :param data_obj:
    :return: The data uri
    :rtype: str

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import convert_data_obj_to_icav2_uri

        # Set vars
        data_obj = get_data_obj_from_data_id("fil.123456")

        # Convert to data uri
        data_uri = convert_data_obj_to_icav2_uri(data_obj)

    """
    return str(
        urlunparse(
            (
                ICAV2_URI_SCHEME,
                data_obj.details.owning_project_id,
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
    Given a data id or uri, convert to a data object

    :param data_id_path_or_uri:
    :param create_data_if_not_found:  If true, create the data if not found (only applicable to paths or uris)

    :return: The data object
    :rtype: `Data <https://umccr-illumina.github.io/libica/openapi/v2/docs/Data/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.data import coerce_data_id_or_icav2_uri_to_data_obj

        # Set vars
        data_id_path_or_uri = "fil.123456"  # Or icav2://project-id/path/to/file  # Or  /path/to/file

        # Coerce to data object
        data_obj = coerce_data_id_or_icav2_uri_to_data_obj(data_id_path_or_uri)
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
