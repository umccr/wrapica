#!/usr/bin/env python3

"""
List of available functions:

"""
# Standard imports
import re
import warnings
from io import TextIOWrapper
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Union, Optional, Any, Tuple, cast
from datetime import datetime
from urllib.parse import urlparse, urlunparse
import requests
from pydantic import UUID4

# Libica imports
from libica.openapi.v3 import ApiClient, RcloneTempCredentials

# Libica Api imports
from libica.openapi.v3 import (
    ApiException,
    CreateFileAndUploadUrl,
    ProjectFileAndUploadUrl,
    ProjectDataMoveBatchApi,
    CreateProjectDataMoveBatch,
    CreateProjectDataMoveBatchItem
)
from libica.openapi.v3.api.project_data_api import ProjectDataApi
from libica.openapi.v3.api.project_data_copy_batch_api import ProjectDataCopyBatchApi

# Libica model imports
from libica.openapi.v3.models import (
    CreateFileData, CreateFolder,
    AnalysisInputExternalData,
    AwsTempCredentials,
    CreateData,
    CreateProjectDataCopyBatch,
    CreateProjectDataCopyBatchItem,
    CreateTemporaryCredentials,
    DataIdOrPathList,
    DataUrlWithPath,
    Download,
    Job,
    ProjectData,
    ProjectDataCopyBatch,
    TempCredentials,
    Upload
)

# Local imports
from ...literals import (
    DataType,
    ProjectDataSortParameterType,
    ProjectDataStatusValuesType,
    UriType,
    DataTagType,
    CredentialsFormat,
)
from ...utils.configuration import get_icav2_configuration, logger
from ...utils.globals import (
    LIBICAV2_DEFAULT_PAGE_SIZE,
    IS_REGEX_MATCH,
    FILE_DATA_TYPE,
    FOLDER_DATA_TYPE,
    ICAV2_URI_SCHEME,
    S3_URI_SCHEME
)
from ...utils.miscell import is_uuid_format, is_uri_format


def get_project_data_file_id_from_project_id_and_path(
        project_id: Union[UUID4, str],
        file_path: Path,
        create_file_if_not_found: bool = False
) -> str:
    """
    Return the file ID for a given project and file path.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param file_path: The absolute path to the file within the project
    :param create_file_if_not_found: If True, creates the file object when not found.
        Defaults to False

    :return: The data identifier string for the file
    :rtype: str

    :raises FileNotFoundError: If the file does not exist and create_file_if_not_found is False
    :raises ApiException: If the API call to list or create project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_file_id_from_project_id_and_path

        file_id = get_project_data_file_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            file_path=Path("/path/to/file.txt")
        )

        print(file_id)
        # fil.1234567890abcdef1234567890abcdef
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    parent_folder_path = str(file_path.parent.absolute()) + "/"
    if parent_folder_path == "//":
        parent_folder_path = "/"

    # Add the filename to the list of filenames to search on
    filename = [
        file_path.name
    ]

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the list of project data.
        data_items: List[ProjectData] = api_instance.get_project_data_list(
            project_id=str(project_id),
            parent_folder_path=parent_folder_path,
            filename=filename,
            filename_match_mode="EXACT",
            file_path_match_mode="FULL_CASE_INSENSITIVE",
            type=FILE_DATA_TYPE
        ).items
    except ApiException as e:
        if not create_file_if_not_found:
            logger.error("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)
            raise ApiException
        else:
            file_obj = create_file_in_project(
                project_id=project_id,
                file_path=file_path.absolute(),
            )
            return file_obj.data.id

    # Get the file id
    try:
        file_id = next(
            filter(
                lambda data_iter: data_iter.data.details.path == str(file_path),
                data_items
            )
        )
    except StopIteration as e:
        if create_file_if_not_found:
            # Create the folder
            file_id = create_file_in_project(
                project_id=project_id,
                file_path=file_path
            )
        else:
            logger.error("Could not find file id for file: %s\n" % file_path)
            raise FileNotFoundError(f"Could not find file id for file: {file_path}") from e

    return file_id.data.id


def create_data_in_project(
        project_id: Union[UUID4, str],
        parent_folder_path: Path,
        data_name: str,
        data_type: DataType
) -> ProjectData:
    """
    Create a data object in a project context.

    .. deprecated:: 2.45.0
        Use :func:`create_file_in_project` or :func:`create_folder_in_project` instead.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param parent_folder_path: The parent folder path where the data object is created
    :param data_name: The name of the file or folder to create
    :param data_type: The data type, one of "FILE" or "FOLDER"

    :return: The newly created project data object
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ApiException: If the API call to create the data object fails
    """
    warnings.warn(
        "create_data_in_project is deprecated and will be removed in a future release. Use create_file_in_project or create_folder_in_project instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    parent_folder_path = str(parent_folder_path.absolute()) + "/"
    if parent_folder_path == "//":
        parent_folder_path = "/"

    # example passing only required values which don't have defaults set
    try:
        # Create a project data.
        # Note that this is a deprecated function and will be removed in the future.
        api_response: ProjectData = api_instance.create_data_in_project(
            project_id=str(project_id),
            create_data=CreateData(
                name=data_name,
                folderPath=parent_folder_path,
                dataType=data_type,
                folderId=None,
                formatCode=None,
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_project_data: %s\n" % e)
        raise ApiException

    # Return the folder id
    return api_response


def create_file_in_project(
        project_id: Union[UUID4, str],
        file_path: Path,
) -> ProjectData:
    """
    Create a file object in a project at the specified path.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param file_path: The absolute path where the file should be created

    :return: The newly created file as a project data object
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ApiException: If the API call to create the file fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import create_file_in_project

        project_data_obj = create_file_in_project(
            project_id="abcd-1234-efab-5678",
            file_path=Path("/path/to/file.txt")
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """

    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    parent_folder_path = str(file_path.absolute().parent) + "/"
    if parent_folder_path == "//":
        parent_folder_path = "/"

    # example passing only required values which don't have defaults set
    try:
        # Create a project data.
        api_response: ProjectData = api_instance.create_file(
            project_id=str(project_id),
            create_file_data=CreateFileData(
                name=file_path.name,
                folderPath=parent_folder_path,
                folderId=None,
                formatCode=None,
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_file: %s\n" % e)
        raise ApiException

    # Return the folder id
    return api_response


def create_folder_in_project(
        project_id: Union[UUID4, str],
        folder_path: Path,
) -> ProjectData:
    """
    Create a folder in a project at the specified path.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_path: The absolute path where the folder should be created

    :return: The newly created folder as a project data object
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ApiException: If the API call to create the folder fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import create_folder_in_project

        project_data_obj = create_folder_in_project(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/new/")
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fol.1234567890abcdef1234567890abcdef, Name: new
    """

    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    parent_folder_path = str(folder_path.absolute().parent) + "/"
    if parent_folder_path == "//":
        parent_folder_path = "/"

    # example passing only required values which don't have defaults set
    try:
        # Create a project data.
        api_response: ProjectData = api_instance.create_folder(
            project_id=str(project_id),
            create_folder=CreateFolder(
                name=folder_path.name,
                folderPath=parent_folder_path,
                folderId=None
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_folder: %s\n" % e)
        raise e

    # Return the folder id
    return api_response


def get_project_data_folder_id_from_project_id_and_path(
        project_id: Union[UUID4, str],
        folder_path: Path,
        create_folder_if_not_found: bool = False
) -> str:
    """
    Return the folder ID for a given project and folder path.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_path: The absolute path to the folder within the project
    :param create_folder_if_not_found: If True, creates the folder when not found.
        Defaults to False

    :return: The data identifier string for the folder
    :rtype: str

    :raises NotADirectoryError: If the folder does not exist and create_folder_if_not_found is False
    :raises ApiException: If the API call to list or create project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_folder_id_from_project_id_and_path

        folder_id = get_project_data_folder_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/")
        )

        print(folder_id)
        # fol.1234567890abcdef1234567890abcdef
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    parent_folder_path = str(folder_path.parent.absolute()) + "/"
    # Exception for when folder is in the top directory
    if parent_folder_path == '//':
        parent_folder_path = '/'

    # Add the folder name to the list of folder names to search on
    folder_name = [
        folder_path.name
    ]

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the list of project data.
        data_items: List[ProjectData] = api_instance.get_project_data_list(
            project_id=str(project_id),
            parent_folder_path=parent_folder_path,
            filename=folder_name,
            filename_match_mode="EXACT",
            file_path_match_mode="FULL_CASE_INSENSITIVE",
            type=FOLDER_DATA_TYPE
        ).items
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)
        raise ApiException

    # Get the folder id
    try:
        folder_id: ProjectData = next(
            filter(
                lambda data_iter: data_iter.data.details.path == str(folder_path) + "/",
                data_items
            )
        )
    except StopIteration:
        if create_folder_if_not_found:
            # Create the folder
            folder_id = create_folder_in_project(
                project_id=project_id,
                folder_path=folder_path
            )
        else:
            logger.error("Could not find folder id for folder: %s\n" % folder_path)
            raise NotADirectoryError

    return folder_id.data.id


def get_project_data_id_from_project_id_and_path(
        project_id: Union[UUID4, str],
        data_path: Path,
        data_type: DataType,
        create_data_if_not_found: bool = False
) -> str:
    """
    Return the data ID for a given project, path, and data type.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_path: The absolute path to the data within the project
    :param data_type: The data type, one of "FILE" or "FOLDER"
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The data identifier string for the file or folder
    :rtype: str

    :raises FileNotFoundError: If data_type is FILE and the file does not exist
    :raises NotADirectoryError: If data_type is FOLDER and the folder does not exist
    :raises ApiException: If the API call to list or create project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_id_from_project_id_and_path

        data_id = get_project_data_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/file.txt"),
            data_type="FILE"
        )

        print(data_id)
        # fil.1234567890abcdef1234567890abcdef
    """
    if data_type == FOLDER_DATA_TYPE:
        return get_project_data_folder_id_from_project_id_and_path(
            project_id=project_id,
            folder_path=data_path,
            create_folder_if_not_found=create_data_if_not_found
        )
    else:
        return get_project_data_file_id_from_project_id_and_path(
            project_id=project_id,
            file_path=data_path,
            create_file_if_not_found=create_data_if_not_found
        )


def get_project_data_obj_by_id(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> ProjectData:
    """
    Return the project data object for a given project and data ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The data identifier as a UUID4 object or data ID string

    :return: The project data object matching the given ID
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import get_project_data_obj_by_id

        project_data_obj = get_project_data_obj_by_id(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """

    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve the list of project data.
        data_obj: ProjectData = api_instance.get_project_data(
            project_id=str(project_id),
            data_id=str(data_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)
        raise ApiException

    return data_obj


def get_project_data_obj_from_project_id_and_path(
        project_id: Union[UUID4, str],
        data_path: Path,
        data_type: DataType,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Return the project data object for a given project, path, and data type.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_path: The absolute path to the data within the project
    :param data_type: The data type, one of "FILE" or "FOLDER"
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object matching the given path
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises FileNotFoundError: If data_type is FILE and the file does not exist
    :raises NotADirectoryError: If data_type is FOLDER and the folder does not exist
    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_obj_from_project_id_and_path

        project_data_obj = get_project_data_obj_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/file.txt"),
            data_type="FILE"
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    # Collect the data id, either fol.id or fil.id
    project_data_id = get_project_data_id_from_project_id_and_path(
        project_id=project_id,
        data_path=data_path,
        data_type=data_type,
        create_data_if_not_found=create_data_if_not_found
    )

    # Then collect the object itself
    return get_project_data_obj_by_id(
        project_id=project_id,
        data_id=project_data_id
    )


def get_project_data_path_by_id(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> Path:
    """
    Return the file system path for a given project and data ID.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The data identifier as a UUID4 object or data ID string

    :return: The absolute path of the data object
    :rtype: Path

    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import get_project_data_path_by_id

        project_data_path = get_project_data_path_by_id(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(project_data_path)
        # /path/to/file.txt
    """
    project_data_path = get_project_data_obj_by_id(
        project_id=project_id,
        data_id=data_id
    ).data.details.path

    return Path(project_data_path)


def list_project_data_non_recursively(
        project_id: Union[UUID4, str],
        parent_folder_id: Optional[Union[UUID4, str]] = None,
        parent_folder_path: Optional[Path] = None,
        file_name: Optional[Union[str, List[str]]] = None,
        status: Optional[Union[ProjectDataStatusValuesType, List[ProjectDataStatusValuesType]]] = None,
        data_type: Optional[DataType] = None,
        creation_date_after: Optional[datetime] = None,
        creation_date_before: Optional[datetime] = None,
        status_date_after: Optional[datetime] = None,
        status_date_before: Optional[datetime] = None,
        sort: Optional[Union[ProjectDataSortParameterType, List[ProjectDataSortParameterType]]] = ""
) -> List[ProjectData]:
    """
    Return a list of data objects directly under a given folder without recursion.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param parent_folder_id: The parent folder identifier. Defaults to None, in which
        case parent_folder_path must be provided
    :param parent_folder_path: The path to the parent folder. Defaults to None, in which
        case parent_folder_id must be provided
    :param file_name: The name or list of names to filter on, supports wildcard.
        Defaults to None
    :param status: The status filter as a single value or list of values.
        Defaults to None
    :param data_type: The data type filter, one of "FILE" or "FOLDER".
        Defaults to None
    :param creation_date_after: Return only data created after this datetime.
        Defaults to None
    :param creation_date_before: Return only data created before this datetime.
        Defaults to None
    :param status_date_after: Return only data with status date after this datetime.
        Defaults to None
    :param status_date_before: Return only data with status date before this datetime.
        Defaults to None
    :param sort: The sort order as a single value or list of sort parameters.
        Defaults to ""

    :return: A list of project data objects in the folder
    :rtype: List[`ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_]

    :raises AssertionError: If both or neither of parent_folder_id and parent_folder_path are provided
    :raises ValueError: If an invalid sort parameter is provided
    :raises ApiException: If the API call to list project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import list_project_data_non_recursively

        project_data_list = list_project_data_non_recursively(
            project_id="abcd-1234-efab-5678",
            parent_folder_path=Path("/path/to/folder/"),
            data_type="FILE"
        )

        for project_data in project_data_list:
            print(f"Data ID: {project_data.data.id}, Name: {project_data.data.details.name}")
            # Data ID: fil.12345678..., Name: file.txt
    """
    # Check one of parent_folder_id and parent_folder_path is specified
    if parent_folder_id is None and parent_folder_path is None:
        logger.error("Must specify one of parent_folder_id and parent_folder_path")
        raise AssertionError
    elif parent_folder_id is not None and parent_folder_path is not None:
        logger.error("Must specify only one of parent_folder_id and parent_folder_path")
        raise AssertionError
    # Specify either parent_folder_id as a list parent_folder_path is just as a string
    if parent_folder_id is not None:
        parent_folder_id = [parent_folder_id]

    # Convert parent folder path to a string
    if parent_folder_path is not None:
        parent_folder_path = str(parent_folder_path.absolute()) + "/"
        if parent_folder_path == "//":
            parent_folder_path = "/"

    # Check file_name
    if isinstance(file_name, str):
        file_name = [file_name]

    # Check status
    if status is not None:
        if isinstance(status, ProjectDataStatusValuesType):
            status = [status]
        elif isinstance(status, str):
            status = [ProjectDataStatusValuesType(status)]
        status = list(
            map(
                lambda status_iter: status_iter.value,
                status
            )
        )

    # Check sort
    if sort == "":
        sort = None

    if sort is not None:
        if isinstance(sort, str) and not sort in ProjectDataSortParameterType.__args__:
            logger.error("Invalid sort parameter provided: %s" % sort)
            raise ValueError("Invalid sort parameter provided")
        elif isinstance(sort, List) and any(map(lambda sort_iter_: sort_iter_ not in ProjectDataSortParameterType.__args__, sort)):
            logger.error("Invalid sort parameter(s) provided: %s" % ", ".join(list(filter(
                lambda sort_iter_: sort_iter_ not in ProjectDataSortParameterType.__args__, sort
            ))))
            raise ValueError("Invalid sort parameter(s) provided")

        if isinstance(sort, str):
            sort = [sort]

        # Complete a comma join of the sort parameters
        sort = ", ".join(sort)

    # Collect api instance
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectDataApi(api_client)

    # Set other parameters
    page_size = LIBICAV2_DEFAULT_PAGE_SIZE
    page_token = ""
    # We use page tokens if sort is None, otherwise we use page offsets
    if sort is not None:
        page_offset = 0
    else:
        page_offset = ""

    # Initialise data ids - we may need to extend the items multiple times
    data_obj_list: List[ProjectData] = []

    # Loop through the pages
    while True:
        # Attempt to collect all data ids
        try:
            # Retrieve the list of project data
            api_response = api_instance.get_project_data_list(
                **dict(
                    filter(
                        lambda x: x[1] is not None,
                        {
                            "status": status,
                            "type": data_type,
                            "project_id": (
                                str(project_id) if project_id is not None else None
                            ),
                            "parent_folder_id": (
                                list(map(str, parent_folder_id)) if parent_folder_id is not None else None
                            ),
                            "parent_folder_path": parent_folder_path,
                            "page_size": str(page_size),
                            "page_offset": str(page_offset),
                            "page_token": page_token,
                            "filename": file_name,
                            "creation_date_after": creation_date_after,
                            "creation_date_before": creation_date_before,
                            "status_date_after": status_date_after,
                            "status_date_before": status_date_before,
                            "sort": sort
                        }.items()
                    )
                )
            )
        except ApiException as e:
            raise ValueError("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)

        # Extend items list
        data_obj_list.extend(api_response.items)

        # Determine page iteration method by if we have a 'sort' parameter
        if sort is not None:
            # Check page offset and page size against total item count
            if page_offset + page_size >= api_response.total_item_count:
                break
            page_offset += page_size
        else:
            # Check if there is a next page
            if api_response.next_page_token is None or api_response.next_page_token == "":
                break
            page_token = api_response.next_page_token

    return data_obj_list


def find_project_data_recursively(
        project_id: Union[UUID4, str],
        parent_folder_id: Optional[Union[UUID4, str]] = None,
        parent_folder_path: Optional[Path] = None,
        name: Optional[str] = None,
        data_type: Optional[DataType] = None,
        min_depth: Optional[int] = None,
        max_depth: Optional[int] = None
) -> List[ProjectData]:
    """
    Return a list of data objects matching criteria by recursing through subdirectories.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param parent_folder_id: The parent folder identifier. Defaults to None, in which
        case parent_folder_path must be provided
    :param parent_folder_path: The path to the parent folder. Defaults to None, in which
        case parent_folder_id must be provided
    :param name: The name or regex pattern to match against. Defaults to None
    :param data_type: The data type filter, one of "FILE" or "FOLDER".
        Defaults to None
    :param min_depth: The minimum folder depth to include results from.
        Defaults to None
    :param max_depth: The maximum folder depth to recurse into.
        Defaults to None

    :return: A list of project data objects matching the search criteria
    :rtype: List[`ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_]

    :raises AssertionError: If both or neither of parent_folder_id and parent_folder_path are provided
    :raises ApiException: If the API call to list project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import find_project_data_recursively

        project_data_list = find_project_data_recursively(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            name="file.txt",
            data_type="FILE",
            max_depth=3
        )

        for project_data in project_data_list:
            print(f"Data ID: {project_data.data.id}, Name: {project_data.data.details.name}")
            # Data ID: fil.12345678..., Name: file.txt
    """
    # Check one of parent_folder_id and parent_folder_path is specified
    if parent_folder_id is None and parent_folder_path is None:
        logger.error("Must specify one of parent_folder_id and parent_folder_path")
        raise AssertionError
    elif parent_folder_id is not None and parent_folder_path is not None:
        logger.error("Must specify only one of parent_folder_id and parent_folder_path")
        raise AssertionError

    # Matched data items thing we return
    matched_data_items: List[ProjectData] = []

    if name is not None and IS_REGEX_MATCH.match(name):
        name_recursive = name  # What we parse to this function recursively
        # If there are any * without a '.' before them, we need to add a '.' before them
        name = re.sub(r"(?<!\.)\*", ".*", name)

        name_regex_obj = re.compile(fr"{name}")
        name = None
    else:
        name_recursive = None
        name_regex_obj = None

    # Get top level items
    data_items: List[ProjectData] = list_project_data_non_recursively(
        project_id=project_id,
        parent_folder_id=parent_folder_id,
        parent_folder_path=parent_folder_path,
        data_type=data_type,
        file_name=name
    )

    # Check if we can pull out any items in the top directory
    if min_depth is None or min_depth <= 1:
        for data_item in data_items:
            # Check data type
            if data_type is not None and not data_item.data.details.data_type == data_type:
                continue
            # Check if we have regex name to match on
            if name_regex_obj is None:
                matched_data_items.append(data_item)
            elif name_regex_obj.fullmatch(data_item.data.details.name) is not None:
                matched_data_items.append(data_item)

    # Otherwise look recursively
    if max_depth is None or not max_depth <= 1:
        # Listing sub folders
        # If we didn't specify the datatype as FILE,
        # or a name / name regex, all the subfolders should be in the data items
        if not data_type == FILE_DATA_TYPE and name is None and name_regex_obj is None:
            subfolders = list(
                filter(
                    lambda x: x.data.details.data_type == FOLDER_DATA_TYPE,
                    data_items
                )
            )
        # Otherwise we will need to regather them
        else:
            subfolders = list_project_data_non_recursively(
                project_id=project_id,
                parent_folder_id=parent_folder_id,
                parent_folder_path=parent_folder_path,
                data_type=FOLDER_DATA_TYPE,
            )
        for subfolder in subfolders:
            matched_data_items.extend(
                find_project_data_recursively(
                    project_id=project_id,
                    parent_folder_id=subfolder.data.id,
                    name=name_recursive,
                    data_type=data_type,
                    min_depth=min_depth - 1 if min_depth is not None else None,
                    max_depth=max_depth - 1 if max_depth is not None else None
                )
            )

    return matched_data_items


def find_project_data_bulk(
        project_id: Union[UUID4, str],
        parent_folder_id: Optional[Union[UUID4, str]] = None,
        parent_folder_path: Optional[Path] = None,
        data_type: Optional[DataType] = None
) -> List[ProjectData]:
    """
    Return all data objects under a folder recursively using bulk listing.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param parent_folder_id: The parent folder identifier. Defaults to None, in which
        case parent_folder_path must be provided
    :param parent_folder_path: The path to the parent folder. Defaults to None, in which
        case parent_folder_id must be provided
    :param data_type: The data type filter, one of "FILE" or "FOLDER".
        Defaults to None

    :return: A list of all project data objects under the folder
    :rtype: List[`ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_]

    :raises AssertionError: If both or neither of parent_folder_id and parent_folder_path are provided
    :raises ApiException: If the API call to list project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import find_project_data_bulk

        project_data_list = find_project_data_bulk(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            data_type="FILE"
        )

        for project_data in project_data_list:
            print(f"Data ID: {project_data.data.id}, Name: {project_data.data.details.name}")
            # Data ID: fil.12345678..., Name: file.txt
    """
    # Check one of parent_folder_id and parent_folder_path is specified
    if parent_folder_id is None and parent_folder_path is None:
        logger.error("Must specify one of parent_folder_id and parent_folder_path")
        raise AssertionError
    elif parent_folder_id is not None and parent_folder_path is not None:
        logger.error("Must specify only one of parent_folder_id and parent_folder_path")
        raise AssertionError

    # Get the parent folder path as a string
    if parent_folder_path is None:
        parent_folder_path = str(get_project_data_path_by_id(project_id, parent_folder_id)) + "/"
    else:
        parent_folder_path = str(parent_folder_path.absolute()) + "/"

    # Initialise
    data_ids: List[ProjectData] = []
    # Collect api instance
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectDataApi(api_client)

    # Set other parameters
    page_size = LIBICAV2_DEFAULT_PAGE_SIZE
    page_token = ""

    # Iterate over all pages
    while True:
        # Attempt to collect all data ids
        try:
            # Retrieve the list of project data
            api_response = api_instance.get_project_data_list(
                project_id=str(project_id),
                file_path=[parent_folder_path],
                file_path_match_mode="STARTS_WITH_CASE_INSENSITIVE",
                page_size=str(page_size),
                page_token=page_token,
                type=data_type
            )

        except ApiException as e:
            logger.error("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)
            raise ApiException

        # Extend items list
        data_ids.extend(api_response.items)

        # Check page offset and page size against total item count
        page_token = api_response.next_page_token

        if page_token == "":
            break

    return data_ids


def create_download_url(
        project_id: Union[UUID4, str],
        file_id: Union[UUID4, str]
) -> str:
    """
    Create a presigned download URL for a file in a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param file_id: The file identifier as a UUID4 object or data ID string

    :return: The presigned download URL string
    :rtype: str

    :raises ApiException: If the API call to create the download URL fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import create_download_url

        download_url = create_download_url(
            project_id="abcd-1234-efab-5678",
            file_id="fil.abcdef1234567890"
        )

        print(download_url)
        # https://stratus-gds-use1.s3.us-east-1.amazonaws.com/path/to/file.txt?signature=abc123
    """
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a download URL for this data.
        api_response: Download = api_instance.create_download_url_for_data(
            project_id=str(project_id),
            data_id=str(file_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_download_url_for_data: %s\n" % e)
        raise ApiException

    return api_response.url


def create_download_urls(
        project_id: Union[UUID4, str],
        folder_id: Union[UUID4, str],
        recursive: bool = False
) -> List[DataUrlWithPath]:
    """
    Create presigned download URLs for all files in a project folder.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_id: The folder identifier as a UUID4 object or data ID string
    :param recursive: If True, includes files in subdirectories. Defaults to False

    :return: A list of download URL objects with path information
    :rtype: List[`DataUrlWithPath <https://umccr.github.io/libica/openapi/v3/docs/DataUrlWithPath/>`_]

    :raises ApiException: If the API call to create download URLs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import create_download_urls

        download_urls = create_download_urls(
            project_id="abcd-1234-efab-5678",
            folder_id="fol.abcdef1234567890",
            recursive=True
        )

        for url_obj in download_urls:
            print(f"Path: {url_obj.path}, URL: {url_obj.url}")
            # Path: /path/to/file.txt, URL: https://stratus-gds-use1.s3.us-east-1.amazonaws.com/...
    """

    if recursive:
        project_data_list = find_project_data_bulk(
            project_id=project_id,
            parent_folder_id=folder_id,
            data_type=FILE_DATA_TYPE
        )
    else:
        project_data_list = list_project_data_non_recursively(
            project_id=project_id,
            parent_folder_id=folder_id,
            data_type=FILE_DATA_TYPE
        )

    # Set data paths
    data_id_paths_list = DataIdOrPathList(
        dataIds=list(
            map(
                lambda project_file_iter: project_file_iter.data.id,
                project_data_list
            )
        ),
        dataPaths=None
    )

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )

    # example passing only required values which don't have defaults set
    try:
        # Retrieve download URLs for the data.
        api_response = api_instance.create_download_urls_for_data(
            project_id=str(project_id),
            data_id_or_path_list=data_id_paths_list
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_download_urls_for_data: %s\n" % e)
        raise ApiException

    # Return items
    return api_response.items


def convert_icav2_uri_to_data_obj(
        data_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Convert an icav2:// URI to a project data object.

    .. deprecated:: 2.45.0
        Use :func:`convert_uri_to_project_data_obj` or
        :func:`wrapica.data.convert_uri_to_data_obj` instead.

    :param data_uri: The icav2:// URI string to convert
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object for the given URI
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails
    """
    return convert_uri_to_project_data_obj(data_uri, create_data_if_not_found)


def convert_icav2_uri_to_project_data_obj(
        data_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Convert an icav2:// URI to a project data object.

    .. deprecated:: 2.45.0
        Use :func:`convert_uri_to_project_data_obj` instead.

    :param data_uri: The icav2:// URI string to convert
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object for the given URI
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails
    """
    return convert_uri_to_project_data_obj(data_uri, create_data_if_not_found)


def convert_uri_to_project_data_obj(
        data_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Convert an icav2:// or s3:// URI to a project data object.

    :param data_uri: The URI string in icav2:// or s3:// format
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object for the given URI
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import convert_uri_to_project_data_obj

        project_data_obj = convert_uri_to_project_data_obj(
            "icav2://project-name/path/to/data/"
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fol.1234567890abcdef1234567890abcdef, Name: data
    """
    # Import other functions locally to avoid circular imports
    from ...project import get_project_id_from_project_name

    data_uri_obj = urlparse(data_uri)

    # Set data type
    if data_uri_obj.path.endswith("/"):
        data_type = cast(DataType, FOLDER_DATA_TYPE)
    else:
        data_type = cast(DataType, FILE_DATA_TYPE)

    if cast(UriType, data_uri_obj.scheme) == ICAV2_URI_SCHEME:
        # Check if the project is in project id format
        if is_uuid_format(data_uri_obj.netloc):
            project_id = data_uri_obj.netloc
        else:
            project_id = get_project_id_from_project_name(data_uri_obj.netloc)
        data_path = Path(data_uri_obj.path)
    elif cast(UriType, data_uri_obj.scheme) == S3_URI_SCHEME:
        # If the uri is an s3 uri, we need to convert it to an icav2 uri
        project_id, data_path = unpack_uri(data_uri)
    else:
        logger.error(f"Could not convert uri to project data object, scheme {data_uri_obj.scheme} not recognised")
        raise ValueError

    # Return the data object
    return get_project_data_obj_from_project_id_and_path(
        project_id=project_id,
        data_path=Path(data_path),
        data_type=data_type,
        create_data_if_not_found=create_data_if_not_found
    )


def convert_project_data_obj_to_icav2_uri(
        project_data: ProjectData
) -> str:
    """
    Convert a project data object to an icav2:// URI string.

    .. deprecated:: 2.45.0
        Use :func:`convert_project_data_obj_to_uri` instead.

    :param project_data: The project data object to convert

    :return: The icav2:// URI string representation
    :rtype: str
    """
    DeprecationWarning(
        "Please use convert_project_data_obj_to_uri instead."
    )
    return convert_project_data_obj_to_uri(project_data, uri_type=ICAV2_URI_SCHEME)


def convert_project_data_obj_to_uri(
        project_data: ProjectData,
        uri_type: UriType = ICAV2_URI_SCHEME
) -> str:
    """
    Convert a project data object to an icav2:// or s3:// URI string.

    :param project_data: The project data object to convert
    :param uri_type: The URI scheme to use, one of "icav2" or "s3".
        Defaults to "icav2"

    :return: The URI string representation of the project data object
    :rtype: str

    :raises ValueError: If the uri_type is not recognised

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import (
            get_project_data_obj_by_id, convert_project_data_obj_to_uri
        )

        project_data_obj = get_project_data_obj_by_id(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        uri = convert_project_data_obj_to_uri(project_data_obj)

        print(uri)
        # icav2://abcd1234-ab12-ab12-ab12-abcdef123456/path/to/file.txt
    """
    from ...storage_configuration import convert_project_data_obj_to_s3_uri
    if uri_type == ICAV2_URI_SCHEME:
        return str(
            urlunparse((
                uri_type,
                str(project_data.project_id),
                project_data.data.details.path.rstrip("/") + (
                    "/" if project_data.data.details.data_type == FOLDER_DATA_TYPE else ""),
                None, None, None
            ))
        )
    elif uri_type == S3_URI_SCHEME:
        return convert_project_data_obj_to_s3_uri(project_data_obj=project_data)
    else:
        logger.error(
            f"Uri type {uri_type} not recognised, please use one of UriType.ICAV2, UriType.S3"
        )
        raise ValueError


def convert_project_id_and_data_path_to_icav2_uri(
        project_id: Union[UUID4, str],
        data_path: Path,
        data_type: DataType
) -> str:
    """
    Convert a project ID and data path to an icav2:// URI string.

    .. deprecated:: 2.45.0
        Use :func:`convert_project_id_and_data_path_to_uri` instead.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_path: The absolute path to the data within the project
    :param data_type: The data type, one of "FILE" or "FOLDER"

    :return: The icav2:// URI string representation
    :rtype: str
    """
    return convert_project_id_and_data_path_to_uri(
        project_id=project_id,
        data_path=data_path,
        data_type=data_type
    )


def convert_project_id_and_data_path_to_uri(
        project_id: Union[UUID4, str],
        data_path: Path,
        data_type: DataType,
        uri_type: UriType = ICAV2_URI_SCHEME
) -> str:
    """
    Convert a project ID and data path to an icav2:// or s3:// URI string.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_path: The absolute path to the data within the project
    :param data_type: The data type, one of "FILE" or "FOLDER"
    :param uri_type: The URI scheme to use, one of "icav2" or "s3".
        Defaults to "icav2"

    :return: The URI string representation of the data path
    :rtype: str

    :raises ValueError: If the uri_type is not recognised

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import convert_project_id_and_data_path_to_uri

        icav2_uri = convert_project_id_and_data_path_to_uri(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/folder/"),
            data_type="FOLDER"
        )

        print(icav2_uri)
        # icav2://abcd1234-ab12-ab12-ab12-abcdef123456/path/to/folder/
    """
    from ...storage_configuration import get_s3_key_prefix_by_project_id
    if uri_type == ICAV2_URI_SCHEME:
        return str(
            urlunparse((
                ICAV2_URI_SCHEME,
                str(project_id),
                str(data_path) + ("/" if data_type == FOLDER_DATA_TYPE else ""),
                None, None, None
            ))
        )
    elif uri_type == S3_URI_SCHEME:
        return str(
            urlunparse((
                S3_URI_SCHEME,
                str(project_id),
                str(
                    Path(get_s3_key_prefix_by_project_id(project_id)) / data_path
                ) + ("/" if data_type == FOLDER_DATA_TYPE else ""),
                None, None, None
            ))
        )
    else:
        logger.error("Error! Could not convert project id and data path to uri, uri scheme {uri_type} not recognised")
        raise ValueError


def unpack_icav2_uri(uri: str) -> Tuple[str, str]:
    """
    Unpack an icav2:// URI into project ID and data path components.

    .. deprecated:: 2.45.0
        Use :func:`unpack_uri` instead.

    :param uri: The icav2:// URI string to unpack

    :return: A tuple of project ID and data path strings
    :rtype: Tuple[str, str]

    :raises ValueError: If the URI scheme is not recognised
    """
    return unpack_uri(uri)


def unpack_uri(uri: str) -> Tuple[str, str]:
    """
    Unpack an icav2:// or s3:// URI into project ID and data path components.

    :param uri: The URI string in icav2:// or s3:// format

    :return: A tuple of project ID and data path strings
    :rtype: Tuple[str, str]

    :raises ValueError: If the URI scheme is not recognised

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import unpack_uri

        project_id, data_path = unpack_uri(
            "icav2://project-name/path/to/file.txt"
        )

        print((project_id, data_path))
        # ('abcd1234-ab12-ab12-ab12-abcdef123456', '/path/to/file.txt')
    """
    # Get local imports
    from ...project import get_project_id_from_project_name
    from ...storage_configuration import unpack_s3_uri

    # Parse obj
    uri_obj = urlparse(uri)

    if cast(UriType, uri_obj.scheme) == ICAV2_URI_SCHEME:
        # Get project name or id
        project_name_or_id = uri_obj.netloc

        # Get data path
        data_path = uri_obj.path

        # Get project id
        if is_uuid_format(project_name_or_id):
            project_id = project_name_or_id
        else:
            project_id = get_project_id_from_project_name(project_name_or_id)

        return project_id, data_path
    elif cast(UriType, uri_obj.scheme) == S3_URI_SCHEME:
        return unpack_s3_uri(uri)
    else:
        raise ValueError(f"Could not unpack uri, scheme {uri_obj.scheme} not recognised")


def coerce_data_id_or_icav2_uri_to_project_data_obj(
        data_id_or_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Coerce a data ID or icav2:// URI to a project data object.

    .. deprecated:: 2.45.0
        Use :func:`coerce_data_id_or_uri_to_project_data_obj` instead.

    :param data_id_or_uri: A data identifier or icav2:// URI string
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object resolved from the input
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails
    """
    return coerce_data_id_or_uri_to_project_data_obj(
        data_id_or_uri=data_id_or_uri,
        create_data_if_not_found=create_data_if_not_found
    )


def coerce_data_id_or_uri_to_project_data_obj(
        data_id_or_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Coerce a data ID or URI string to a project data object.

    :param data_id_or_uri: A data identifier string or URI in icav2:// or s3:// format
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object resolved from the input
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import coerce_data_id_or_uri_to_project_data_obj

        project_data_obj = coerce_data_id_or_uri_to_project_data_obj(
            data_id_or_uri="icav2://project-name/path/to/data/"
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fol.1234567890abcdef1234567890abcdef, Name: data
    """
    from ...data import get_project_data_obj_from_data_id
    if is_data_id_format(
            data_id=data_id_or_uri
    ):
        return get_project_data_obj_from_data_id(
            data_id=data_id_or_uri
        )
    return convert_uri_to_project_data_obj(
        data_uri=data_id_or_uri,
        create_data_if_not_found=create_data_if_not_found
    )


def coerce_data_id_icav2_uri_or_path_to_project_data_obj(
        data_id_path_or_uri: str,
        create_data_if_not_found: bool = False
) -> Optional[ProjectData]:
    """
    Coerce a data ID, icav2:// URI, or path to a project data object.

    .. deprecated:: 2.45.0
        Use :func:`coerce_data_id_uri_or_path_to_project_data_obj` or
        :func:`wrapica.data.coerce_data_id_uri_or_path_to_data_obj` instead.

    :param data_id_path_or_uri: A data identifier, icav2:// URI, or path string
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object, or None if the path is root
    :rtype: Optional[`ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_]

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails
    """
    return coerce_data_id_uri_or_path_to_project_data_obj(data_id_path_or_uri, create_data_if_not_found)


def coerce_data_id_uri_or_path_to_project_data_obj(
        data_id_path_or_uri: str,
        create_data_if_not_found: bool = False
) -> Optional[ProjectData]:

    """
    Coerce a data ID, URI, or path string to a project data object.

    :param data_id_path_or_uri: A data identifier, URI, or absolute path string
    :param create_data_if_not_found: If True, creates the data object when not found.
        Defaults to False

    :return: The project data object, or None if the path is root
    :rtype: Optional[`ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_]

    :raises ValueError: If the URI scheme is not recognised
    :raises ApiException: If the API call to retrieve the project data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import coerce_data_id_uri_or_path_to_project_data_obj

        project_data_obj = coerce_data_id_uri_or_path_to_project_data_obj(
            data_id_path_or_uri="icav2://project-name/path/to/data/"
        )

        print(f"Data ID: {project_data_obj.data.id}, Name: {project_data_obj.data.details.name}")
        # Data ID: fol.1234567890abcdef1234567890abcdef, Name: data
    """
    from ...project import get_project_id

    if is_data_id_format(data_id_path_or_uri):
        # Data ID, easy to convert across
        return get_project_data_obj_by_id(
            project_id=get_project_id(),
            data_id=data_id_path_or_uri
        )
    elif (
            is_uri_format(data_id_path_or_uri) and
            cast(UriType, urlparse(data_id_path_or_uri).scheme) in [ICAV2_URI_SCHEME, S3_URI_SCHEME]
    ):
        # ICAv2 URI, convert to data object
        return convert_uri_to_project_data_obj(
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
        return project_data_obj


def get_credentials_access_for_project_folder(
        project_id: Union[UUID4, str],
        folder_id: Optional[Union[UUID4, str]] = None,
        folder_path: Optional[Path] = None,
        read_only: Optional[bool] = None,
        credentials_format: Optional[CredentialsFormat] = None
) -> Union[AwsTempCredentials, RcloneTempCredentials]:
    """
    Retrieve temporary access credentials for a folder within a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_id: The folder identifier. Defaults to None, in which case
        folder_path must be provided
    :param folder_path: The path to the folder within the project. Defaults to None,
        in which case folder_id must be provided
    :param read_only: If True, requests read-only credentials. Defaults to None
    :param credentials_format: The credential format, one of None or "RCLONE".
        Defaults to None, which returns AWS format credentials

    :return: Temporary credentials in AWS or RCLONE format
    :rtype: Union[`AwsTempCredentials <https://umccr.github.io/libica/openapi/v3/docs/AwsTempCredentials/>`_, `RcloneTempCredentials <https://umccr.github.io/libica/openapi/v3/docs/RcloneTempCredentials/>`_]

    :raises AssertionError: If both or neither of folder_id and folder_path are provided
    :raises ValueError: If valid credentials cannot be retrieved from the API

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_credentials_access_for_project_folder

        credentials = get_credentials_access_for_project_folder(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/"),
            read_only=True
        )

        print(f"Access Key: {credentials.access_key}, Region: {credentials.region}")
        # Access Key: AKIAIOSFODNN7EXAMPLE, Region: us-east-1
    """
    # Check one of folder_id and folder_path is specified
    if folder_id is None and folder_path is None:
        logger.error("Must specify one of folder_id and folder_path")
        raise AssertionError
    elif folder_id is not None and folder_path is not None:
        logger.error("Must specify only one of folder_id and folder_path")
        raise AssertionError

    if folder_id is None:
        folder_id = get_project_data_folder_id_from_project_id_and_path(
            project_id=project_id,
            folder_path=folder_path
        )

    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )

    # Create the temp credentials object
    create_temporary_credentials = CreateTemporaryCredentials(
        **dict(filter(
            lambda kv_iter_: kv_iter_[1] is not None,
            {
                "credentialsFormat": credentials_format,
                "readOnlyCredentials": read_only
            }.items()
        ))
    )

    # example passing only required values which don't have defaults set
    try:
        # Retrieve temporary credentials for this data.
        api_response: TempCredentials = api_instance.create_temporary_credentials_for_data(
            project_id=str(project_id),
            data_id=str(folder_id),
            create_temporary_credentials=create_temporary_credentials
        )
    except ApiException as e:
        logger.warning("Exception when calling ProjectDataApi->create_temporary_credentials_for_data: %s\n" % e)
        raise ValueError

    if credentials_format is not None and credentials_format == 'RCLONE':
        if api_response.rclone_temp_credentials is None:
            raise ValueError("Could not retrieve valid RCLONE credentials, no RCLONE credentials returned from API")
        return api_response.rclone_temp_credentials

    if credentials_format is None and api_response.aws_temp_credentials is not None:
        if api_response.aws_temp_credentials is None:
            raise ValueError("Could not retrieve valid AWS credentials, no AWS credentials returned from API")
        return api_response.aws_temp_credentials

    raise ValueError("Could not retrieve valid credentials, no credentials returned from API in either RCLONE or cloud native format")


def get_aws_credentials_access_for_project_folder(
        project_id: Union[UUID4, str],
        folder_id: Optional[Union[UUID4, str]] = None,
        folder_path: Optional[Path] = None,
        read_only: Optional[bool] = None
) -> AwsTempCredentials:
    """
    Retrieve AWS temporary credentials for accessing a project folder.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_id: The folder identifier. Defaults to None, in which case
        folder_path must be provided
    :param folder_path: The path to the folder within the project. Defaults to None,
        in which case folder_id must be provided
    :param read_only: If True, requests read-only credentials. Defaults to None

    :return: An AWS temporary credentials object with access key, secret, and token
    :rtype: `AwsTempCredentials <https://umccr.github.io/libica/openapi/v3/docs/AwsTempCredentials/>`_

    :raises AssertionError: If both or neither of folder_id and folder_path are provided
    :raises ValueError: If valid AWS credentials cannot be retrieved from the API

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_aws_credentials_access_for_project_folder

        aws_creds = get_aws_credentials_access_for_project_folder(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/"),
            read_only=True
        )

        print(f"Access Key: {aws_creds.access_key}, Region: {aws_creds.region}")
        # Access Key: AKIAIOSFODNN7EXAMPLE, Region: us-east-1
    """
    return get_credentials_access_for_project_folder(
        project_id=project_id,
        folder_id=folder_id,
        folder_path=folder_path,
        read_only=read_only,
        credentials_format=None
    )



def get_rclone_credentials_access_for_project_folder(
        project_id: Union[UUID4, str],
        folder_id: Optional[Union[UUID4, str]] = None,
        folder_path: Optional[Path] = None,
        read_only: Optional[bool] = None
) -> RcloneTempCredentials:
    """
    Retrieve Rclone temporary credentials for accessing a project folder.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_id: The folder identifier. Defaults to None, in which case
        folder_path must be provided
    :param folder_path: The path to the folder within the project. Defaults to None,
        in which case folder_id must be provided
    :param read_only: If True, requests read-only credentials. Defaults to None

    :return: An Rclone temporary credentials object for remote access
    :rtype: `RcloneTempCredentials <https://umccr.github.io/libica/openapi/v3/docs/RcloneTempCredentials/>`_

    :raises AssertionError: If both or neither of folder_id and folder_path are provided
    :raises ValueError: If valid RCLONE credentials cannot be retrieved from the API

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_rclone_credentials_access_for_project_folder

        rclone_creds = get_rclone_credentials_access_for_project_folder(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/"),
            read_only=True
        )

        print(f"Type: {rclone_creds.type}, Region: {rclone_creds.region}")
        # Type: s3, Region: us-east-1
    """
    return get_credentials_access_for_project_folder(
        project_id=project_id,
        folder_id=folder_id,
        folder_path=folder_path,
        read_only=read_only,
        credentials_format='RCLONE'
    )


def is_folder_id_format(
        folder_id_str: str
) -> bool:
    """
    Check if a string matches the folder ID format.

    :param folder_id_str: The string to check against folder ID pattern

    :return: True if the string matches the folder ID format
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import is_folder_id_format

        print(is_folder_id_format("fol.abcdef1234567890"))
        # True
    """
    return re.match("fol.[0-9a-f]{32}", folder_id_str) is not None


def is_file_id_format(
        file_id_str: str
) -> bool:
    """
    Check if a string matches the file ID format.

    :param file_id_str: The string to check against file ID pattern

    :return: True if the string matches the file ID format
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import is_file_id_format

        print(is_file_id_format("fil.abcdef1234567890"))
        # True
    """
    return re.match("fil.[0-9a-f]{32}", file_id_str) is not None


def is_data_id_format(
        data_id: Union[UUID4, str]
) -> bool:
    """
    Check if a string matches either a file or folder ID format.

    :param data_id: The string to check against data ID patterns

    :return: True if the string matches a file or folder ID format
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import is_data_id_format

        print(is_data_id_format("fil.abcdef1234567890"))
        # True
    """
    return is_file_id_format(data_id) or is_folder_id_format(data_id)


def check_folder_exists(
        project_id: Union[UUID4, str],
        folder_path: Path
) -> bool:
    """
    Check if a folder exists at the given path in a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_path: The absolute path to the folder within the project

    :return: True if the folder exists, False otherwise
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import check_folder_exists

        exists = check_folder_exists(
            "abcd-1234-efab-5678", Path("/path/to/folder/")
        )

        print(exists)
        # True
    """
    try:
        # Try to get data object from project id and path
        get_project_data_obj_from_project_id_and_path(project_id, folder_path, data_type=FOLDER_DATA_TYPE)
    except (ValueError, FileNotFoundError):
        return False
    else:
        return True


def check_file_exists(
        project_id: Union[UUID4, str],
        file_path: Path
) -> bool:
    """
    Check if a file exists at the given path in a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param file_path: The absolute path to the file within the project

    :return: True if the file exists, False otherwise
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import check_file_exists

        exists = check_file_exists(
            "abcd-1234-efab-5678", Path("/path/to/file.txt")
        )

        print(exists)
        # True
    """
    try:
        # Try to get data object from project id and path
        get_project_data_obj_from_project_id_and_path(project_id, file_path, data_type=FILE_DATA_TYPE)
    except (ValueError, FileNotFoundError):
        return False
    else:
        return True


def check_uri_exists(
        data_uri: str
) -> bool:
    """
    Check if an icav2:// or s3:// URI points to existing data.

    :param data_uri: The URI string in icav2:// or s3:// format

    :return: True if the data at the URI exists, False otherwise
    :rtype: bool

    :raises ValueError: If the URI scheme is not supported

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import check_uri_exists

        print(check_uri_exists("icav2://project-name/path/to/file.txt"))
        # True
    """
    if cast(UriType, urlparse(data_uri).scheme) in [ICAV2_URI_SCHEME, S3_URI_SCHEME]:
        project_id, data_path = unpack_uri(data_uri)
    else:
        raise ValueError(f"URI scheme '{urlparse(data_uri).scheme}' not supported")
    if data_path.endswith("/"):
        return check_folder_exists(project_id, Path(data_path))
    else:
        return check_file_exists(project_id, Path(data_path))


def presign_folder(
        project_id: Union[UUID4, str],
        folder_path: Optional[Path] = None,
        folder_id: Optional[Union[UUID4, str]] = None
) -> List[DataUrlWithPath]:
    """
    Create presigned download URLs for all files in a folder recursively.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_path: The absolute path to the folder. Defaults to None, in which
        case folder_id must be provided
    :param folder_id: The folder identifier. Defaults to None, in which case
        folder_path must be provided

    :return: A list of presigned download URL objects with path information
    :rtype: List[`DataUrlWithPath <https://umccr.github.io/libica/openapi/v3/docs/DataUrlWithPath/>`_]

    :raises ApiException: If the API call to create download URLs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import presign_folder

        presigned_urls = presign_folder(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/")
        )

        for url_obj in presigned_urls:
            print(f"Path: {url_obj.path}, URL: {url_obj.url}")
            # Path: /path/to/file.txt, URL: https://stratus-gds-use1.s3.us-east-1.amazonaws.com/...
    """

    if folder_id is None:
        folder_id = get_project_data_id_from_project_id_and_path(
            project_id=project_id,
            data_path=folder_path,
            data_type=FOLDER_DATA_TYPE
        )

    return create_download_urls(
        project_id=project_id,
        folder_id=folder_id,
        recursive=True
    )


def presign_cwl_directory(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> List[
    Union[
        Dict[str, Union[Union[dict, str], Any]],
        Dict[str, Union[str, Any]]
    ]
]:
    """
    Create a CWL directory listing with presigned URLs for all files.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The folder identifier for the CWL directory

    :return: A CWL directory listing with presigned URLs as file locations
    :rtype: List[Dict[str, Any]]

    :raises ApiException: If the API call to list project data or create URLs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import presign_cwl_directory

        cwl_directory = presign_cwl_directory(
            project_id="abcd-1234-efab-5678",
            data_id="fol.abcdef1234567890"
        )

        for item in cwl_directory:
            print(f"Class: {item['class']}, Basename: {item['basename']}")
            # Class: File, Basename: file.txt
    """
    # Data ids
    cwl_item_objs = []

    # List items noncursively
    file_obj_list = list_project_data_non_recursively(
        project_id=project_id,
        parent_folder_id=data_id
    )

    # Collect file object list
    for file_item_obj in file_obj_list:
        data_type = cast(DataType, file_item_obj.data.details.data_type) # One of FILE | FOLDER
        data_id = file_item_obj.data.id
        basename = file_item_obj.data.details.name
        if data_type == FOLDER_DATA_TYPE:
            cwl_item_objs.append(
                {
                    "class": "Directory",
                    "basename": basename,
                    "listing": presign_cwl_directory(project_id, data_id)
                }
            )
        else:
            cwl_item_objs.append(
                {
                    "class": "File",
                    "basename": basename,
                    "location": create_download_url(project_id, data_id)
                }
            )

    return cwl_item_objs


def presign_cwl_directory_with_external_data_mounts(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> Tuple[
    # External data mounts
    List[AnalysisInputExternalData],
        # Dict listing
    List[Dict]
]:
    """
    Create a CWL directory listing with external data mount objects for analysis input.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The folder identifier for the CWL directory

    :return: A tuple of external data mount objects and CWL directory listing dicts
    :rtype: Tuple[List[`AnalysisInputExternalData <https://umccr.github.io/libica/openapi/v3/docs/AnalysisInputExternalData/>`_], List[Dict]]

    :raises ApiException: If the API call to list project data or create URLs fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import presign_cwl_directory_with_external_data_mounts

        external_mounts, cwl_listing = presign_cwl_directory_with_external_data_mounts(
            project_id="abcd-1234-efab-5678",
            data_id="fol.abcdef1234567890"
        )

        print(f"Found {len(external_mounts)} mount(s) and {len(cwl_listing)} listing(s)")
        # Found 3 mount(s) and 3 listing(s)
    """
    # Data ids
    cwl_item_objs = []

    # External data mounts
    external_data_mounts = []

    # List items noncursively
    file_obj_list = list_project_data_non_recursively(
        project_id=project_id,
        parent_folder_id=data_id
    )

    # Collect file object list
    for file_item_obj in file_obj_list:
        data_type = cast(DataType, file_item_obj.data.details.data_type)  # One of FILE | FOLDER
        data_id = file_item_obj.data.id
        basename = file_item_obj.data.details.name
        if data_type == FOLDER_DATA_TYPE:
            external_data_mounts_new, listing = presign_cwl_directory_with_external_data_mounts(
                project_id,
                data_id
            )
            external_data_mounts.extend(external_data_mounts_new)
            cwl_item_objs.append(
                {
                    "class": "Directory",
                    "basename": basename,
                    "listing": listing
                }
            )
        else:
            # Generate presigned url
            presigned_url = create_download_url(project_id, data_id)

            # Generate mount path for file
            mount_path = str(
                Path(project_id) /
                Path(data_id) /
                Path(basename)
            )

            # Append the mount path and presigned url to the external data mounts list
            external_data_mounts.append(
                AnalysisInputExternalData(
                    url=presigned_url,
                    type="http",
                    mountPath=mount_path,
                    s3Details=None,
                    basespaceDetails=None,
                )
            )

            # Append the item to the cwl item object list
            cwl_item_objs.append(
                {
                    "class": "File",
                    "basename": basename,
                    "location": mount_path
                }
            )

    return external_data_mounts, cwl_item_objs


def read_icav2_file_contents(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str],
        output_path: Optional[Union[Path, TextIOWrapper]] = None
) -> Optional[str]:
    """
    Read file contents from ICAv2 and write to a path or return as string.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The file data identifier to read contents from
    :param output_path: The output path or file handle to write contents to.
        Defaults to None, in which case the contents are returned as a string

    :return: The file contents as a string if output_path is None, otherwise None
    :rtype: Optional[str]

    :raises NotADirectoryError: If the output path parent directory does not exist
    :raises ApiException: If the API call to create the download URL fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import read_icav2_file_contents

        contents = read_icav2_file_contents(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(contents)
        # Hello, World!
    """
    if output_path is not None and isinstance(output_path, Path):
        # Ensure parent directory exists
        if not output_path.parent.exists():
            logger.error(f"Could not write to output path {output_path} as the parent directory does not exist")
            raise NotADirectoryError

    # Get the presigned url
    presigned_url = create_download_url(project_id, data_id)

    # Get the file contents with the requests package
    r = requests.get(presigned_url)

    if output_path is None:
        return r.content.decode()
    elif isinstance(output_path, Path):
        # Write the file contents to the output path
        with open(output_path, "wb") as f:
            f.write(r.content)
    else:
        # Write the file contents to the output path
        output_path.write(r.content.decode())

    return None


def read_icav2_file_contents_to_string(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> str:
    """
    Download and return the contents of an ICAv2 file as a string.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The file data identifier to read contents from

    :return: The file contents as a decoded string
    :rtype: str

    :raises ApiException: If the API call to create the download URL fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import read_icav2_file_contents_to_string

        file_contents = read_icav2_file_contents_to_string(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(file_contents)
        # Hello, World!
    """

    with NamedTemporaryFile() as temp_file_h:
        read_icav2_file_contents(
            project_id=project_id,
            data_id=data_id,
            output_path=Path(temp_file_h.name)
        )

        with open(temp_file_h.name, "r") as f:
            return f.read()


def create_file_with_upload_url(
        project_id: Union[UUID4, str],
        folder_id: Union[UUID4, str],
        file_name: str
) -> str:
    """
    Create a new file in a project folder and return its upload URL.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param folder_id: The folder identifier to create the file in
    :param file_name: The name of the file to create

    :return: The presigned upload URL for the new file
    :rtype: str

    :raises ApiException: If the API call to create the file or upload URL fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import create_file_with_upload_url

        upload_url = create_file_with_upload_url(
            project_id="abcd-1234-efab-5678",
            folder_id="fol.abcdef1234567890",
            file_name="output.txt"
        )

        print(upload_url)
        # https://stratus-gds-use1.s3.us-east-1.amazonaws.com/path/to/output.txt?signature=abc123
    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve an upload URL for this data.
        api_response: ProjectFileAndUploadUrl = api_instance.create_file_with_upload_url(
            project_id=str(project_id),
            create_file_and_upload_url=CreateFileAndUploadUrl(
                name=file_name,
                folderId=str(folder_id),
                folderPath=None,
                formatCode=None,
                fileType=None,
                hash=None,
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_upload_url_for_data: %s\n" % e)
        raise ApiException("Exception when calling ProjectDataApi->create_upload_url_for_data: %s\n" % e) from e

    return api_response.upload_url


def get_project_data_upload_url(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> str:
    """
    Return an upload URL for a project data object that has not yet been written to.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The data identifier for the file to upload to

    :return: The presigned upload URL for the data object
    :rtype: str

    :raises ApiException: If the API call to create the upload URL fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import get_project_data_upload_url

        upload_url = get_project_data_upload_url(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(upload_url)
        # https://stratus-gds-use1.s3.us-east-1.amazonaws.com/path/to/file.txt?signature=abc123
    """

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve an upload URL for this data.
        api_response: Upload = api_instance.create_upload_url_for_data(
            project_id=str(project_id),
            data_id=str(data_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_upload_url_for_data: %s\n" % e)
        raise ApiException

    return api_response.url


def write_icav2_file_contents(
        project_id: Union[UUID4, str],
        data_path: Path,
        file_stream_or_path: Union[Path, TextIOWrapper]
) -> str:
    """
    Write local file contents to a new ICAv2 file at the specified path.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_path: The absolute path for the new file in the project
    :param file_stream_or_path: The local file path or open file handle to upload from

    :return: The data identifier of the newly created file
    :rtype: str

    :raises ApiException: If the API call to create or upload the file fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import write_icav2_file_contents

        new_file_id = write_icav2_file_contents(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/file.txt"),
            file_stream_or_path=Path("/local/file.txt")
        )

        print(new_file_id)
        # fil.1234567890abcdef1234567890abcdef
    """

    # Generate a new file in the project
    new_file_obj = create_file_in_project(
        project_id=project_id,
        file_path=data_path
    )

    # Get the upload url
    upload_url = get_project_data_upload_url(
        project_id=project_id,
        data_id=new_file_obj.data.id
    )

    if isinstance(file_stream_or_path, Path):
        with open(file_stream_or_path, "rb") as f:
            file_contents = f.read()
    else:
        file_contents = file_stream_or_path.read()

    # Upload file contents with the requests package
    requests.put(upload_url, data=file_contents)

    # Return the new file id
    return new_file_obj.data.id


def get_file_by_file_name_from_project_data_list(
        file_name: str,
        project_data_list: List[ProjectData]
) -> ProjectData:
    """
    Return the first file matching the given name from a list of project data objects.

    :param file_name: The name of the file to search for
    :param project_data_list: The list of project data objects to search through

    :return: The first project data object matching the file name
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If no file with the given name is found in the list

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import (
            find_project_data_bulk, get_file_by_file_name_from_project_data_list
        )

        project_data_list = find_project_data_bulk(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            data_type="FILE"
        )
        file_obj = get_file_by_file_name_from_project_data_list(
            file_name="file.txt",
            project_data_list=project_data_list
        )

        print(f"Data ID: {file_obj.data.id}, Name: {file_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """

    # Find the first file with this name
    try:
        return next(
            filter(
                lambda file_iter: (
                        file_iter.data.details.name == file_name and
                        file_iter.data.details.data_type == FILE_DATA_TYPE
                ),
                project_data_list
            )
        )
    except StopIteration:
        logger.error(f"Could not get file {file_name} from analysis output")
        raise ValueError


def project_data_copy_batch_handler(
        source_data_ids: List[Union[UUID4, str]],
        destination_project_id: Union[UUID4, str],
        destination_folder_path: Path
) -> Job:
    """
    Copy a batch of data items to a destination folder in a project.

    :param source_data_ids: The list of source data identifiers to copy
    :param destination_project_id: The destination project identifier
    :param destination_folder_path: The destination folder path in the target project

    :return: The job object for the copy batch operation
    :rtype: `Job <https://umccr.github.io/libica/openapi/v3/docs/Job/>`_

    :raises ApiException: If the API call to create the copy batch fails

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import project_data_copy_batch_handler

        job = project_data_copy_batch_handler(
            source_data_ids=["fil.abcdef1234567890", "fil.abcdef1234567891"],
            destination_project_id="abcd-1234-efab-5678",
            destination_folder_path=Path("/path/to/folder/")
        )

        print(f"Job ID: {job.id}, Status: {job.status}")
        # Job ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: RUNNING
    """

    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataCopyBatchApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Copy a batch of project data.
        api_response: ProjectDataCopyBatch = api_instance.create_project_data_copy_batch(
            project_id=str(destination_project_id),
            create_project_data_copy_batch=CreateProjectDataCopyBatch(
                items=list(
                    map(
                        lambda source_data_id_iter: CreateProjectDataCopyBatchItem(
                            dataId=str(source_data_id_iter)
                        ),
                        source_data_ids
                    )
                ),
                destinationFolderId=get_project_data_folder_id_from_project_id_and_path(
                    project_id=destination_project_id,
                    folder_path=destination_folder_path,
                    create_folder_if_not_found=True
                ),
                copyUserTags=True,
                copyTechnicalTags=True,
                copyInstrumentInfo=True,
                actionOnExist="SKIP",
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->copy_project_data_batch: %s\n" % e)
        raise ApiException

    # Return the job object for the project data copy batch
    return cast(Job, api_response.job)


def delete_project_data(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
):
    """
    Schedule a project data item for deletion.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The data identifier of the item to delete

    :raises ApiException: If the API call to delete the data fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import delete_project_data

        # Schedules the data item for deletion
        delete_project_data(
            project_id="abcd-1234-efab-5678",
            data_id="fol.abcdef1234567890"
        )
    """
    # Get the configuration
    configuration = get_icav2_configuration()

    # Enter a context with an instance of the API client
    with ApiClient(configuration) as api_client:
        # Force default headers for endpoints with a ':' in the name
        api_client.set_default_header(
            header_name="Content-Type",
            header_value="application/vnd.illumina.v3+json"
        )
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Schedule this data for deletion.
        api_instance.delete_data(
            project_id=str(project_id),
            data_id=str(data_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->delete_data: %s\n" % e)
        raise ApiException


def move_project_data(
        dest_project_id: Union[UUID4, str],
        dest_folder_id: Union[UUID4, str],
        src_data_list: List[Union[UUID4, str]]
) -> Job:
    """
    Move a list of data items to a destination folder in a project.

    :param dest_project_id: The destination project identifier as a UUID4 object or UUID-formatted string
    :param dest_folder_id: The destination folder identifier to move data into
    :param src_data_list: The list of source data identifiers to move

    :return: The job object for the move batch operation
    :rtype: `Job <https://umccr.github.io/libica/openapi/v3/docs/Job/>`_

    :raises ApiException: If the API call to create the move batch fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import move_project_data

        job = move_project_data(
            dest_project_id="abcd-1234-efab-5678",
            dest_folder_id="fol.abcdef1234567890",
            src_data_list=["fil.abcdef1234567890", "fil.abcdef1234567891"]
        )

        print(f"Job ID: {job.id}, Status: {job.status}")
        # Job ID: abcd1234-ab12-ab12-ab12-abcdef123456, Status: RUNNING
    """

    # Create an instance of the API class
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectDataMoveBatchApi(api_client)

    try:
        # Create a project data copy batch.
        api_response = api_instance.create_project_data_move_batch(
            project_id=str(dest_project_id),
            create_project_data_move_batch=CreateProjectDataMoveBatch(
                items=list(
                    map(
                        lambda src_data_iter: CreateProjectDataMoveBatchItem(
                            dataId=str(src_data_iter)
                        ),
                        src_data_list
                    )
                ),
                destinationFolderId=str(dest_folder_id),
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataMoveBatchApi->create_project_data_move_batch: %s\n" % e)
        raise ApiException("Exception when calling ProjectDataMoveBatchApi->create_project_data_move_batch: %s\n") from e

    # Get job from job id
    return api_response.job


def copy_project_data(
        dest_project_id: Union[UUID4, str],
        dest_folder_id: Union[UUID4, str],
        src_data_list: List[Union[UUID4, str]]
) -> Job:
    """
    Copy a list of data ids to a destination project
    :param dest_project_id:
    :param dest_folder_id:
    :param src_data_list:

    :return:

    :rtype: Job

    :raises: ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project_data import copy_project_data

        job = copy_project_data(
            dest_project_id="abcd-1234-efab-5678",
            dest_folder_id="fol.abcdef1234567890",
            src_data_list=[
                "fil.abcdef1234567890",
                "fil.abcdef1234567891"
            ]
        )

    """

    # Create an instance of the API class
    with ApiClient(get_icav2_configuration()) as api_client:
        api_instance = ProjectDataCopyBatchApi(api_client)

    try:
        # Create a project data copy batch.
        api_response = api_instance.create_project_data_copy_batch(
            project_id=str(dest_project_id),
            create_project_data_copy_batch=CreateProjectDataCopyBatch(
                items=list(
                    map(
                        lambda src_data_iter: CreateProjectDataCopyBatchItem(
                            dataId=str(src_data_iter)
                        ),
                        src_data_list
                    )
                ),
                destinationFolderId=str(dest_folder_id),
                copyUserTags=False,
                copyTechnicalTags=False,
                copyInstrumentInfo=False,
                actionOnExist="SKIP"  # SKIP | OVERWRITE | FAIL
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataCopyBatchApi->create_project_data_copy_batch: %s\n" % e)
        raise ApiException("Exception when calling ProjectDataCopyBatchApi->create_project_data_copy_batch: %s\n") from e

    # Get job from job id
    return api_response.job


def update_project_data_obj(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str],
        project_data_obj: ProjectData
):
    """
    Given a project id, and data id, update with the project data object
    :param project_id:
    :param data_id:
    :param project_data_obj:
    :return:
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    try:
        # Update the project data object
        api_response: ProjectData = api_instance.update_project_data(
            project_id=str(project_id),
            data_id=str(data_id),
            project_data=project_data_obj
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->update_project_data: %s\n" % e)
        raise e

    return api_response


def add_tag_to_data_object(
        project_id: Union[UUID4, str],
        data_id: Union[UUID4, str],
        tag: str,
        tag_type: DataTagType
) -> ProjectData:
    """
    Add a tag of the specified type to a project data object.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string
    :param data_id: The data identifier of the item to tag
    :param tag: The tag value string to add
    :param tag_type: The tag type, one of "technical_tag", "user_tag", "connector_tag",
        "run_in_tag", "run_out_tag", or "reference_tag"

    :return: The updated project data object with the new tag
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_

    :raises ValueError: If the tag_type is not recognised
    :raises ApiException: If the API call to update the data object fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import add_tag_to_data_object

        updated_obj = add_tag_to_data_object(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890",
            tag="to_be_archived",
            tag_type="user_tag"
        )

        print(f"Data ID: {updated_obj.data.id}, Name: {updated_obj.data.details.name}")
        # Data ID: fil.1234567890abcdef1234567890abcdef, Name: file.txt
    """
    # Get the existing object
    project_data_obj = get_project_data_obj_by_id(
        project_id=project_id,
        data_id=data_id
    )

    # Get existing tags
    if tag_type == "technical_tag":
        project_data_obj.data.details.tags.technical_tags.append(tag)
    elif tag_type == "user_tag":
        project_data_obj.data.details.tags.user_tags.append(tag)
    elif tag_type == "connector_tag":
        project_data_obj.data.details.tags.connector_tags.append(tag)
    elif tag_type == "run_in_tag":
        project_data_obj.data.details.tags.run_in_tags.append(tag)
    elif tag_type == "run_out_tag":
        project_data_obj.data.details.tags.run_out_tags.append(tag)
    elif tag_type == "reference_tag":
        project_data_obj.data.details.tags.reference_tags.append(tag)
    else:
        raise ValueError("Tag type not recognised")

    # Update the analysis object
    project_data_obj = update_project_data_obj(
        project_id=project_id,
        data_id=data_id,
        project_data_obj=project_data_obj
    )

    return project_data_obj
