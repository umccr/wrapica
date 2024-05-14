#!/usr/bin/env python3

"""
List of available functions:

"""

# Standard imports
import re
from io import TextIOWrapper
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Union, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urlunparse

import requests
# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.project_data_api import ProjectDataApi
from libica.openapi.v2.api.project_data_copy_batch_api import ProjectDataCopyBatchApi
from libica.openapi.v2.model.analysis_input_external_data import AnalysisInputExternalData
from libica.openapi.v2.model.aws_temp_credentials import AwsTempCredentials
from libica.openapi.v2.model.create_data import CreateData
from libica.openapi.v2.model.create_project_data_copy_batch import CreateProjectDataCopyBatch
from libica.openapi.v2.model.create_project_data_copy_batch_item import CreateProjectDataCopyBatchItem
from libica.openapi.v2.model.create_temporary_credentials import CreateTemporaryCredentials
from libica.openapi.v2.model.data_id_or_path_list import DataIdOrPathList
from libica.openapi.v2.model.data_url_with_path import DataUrlWithPath
from libica.openapi.v2.model.download import Download
from libica.openapi.v2.model.job import Job
from libica.openapi.v2.model.project_data import ProjectData
from libica.openapi.v2.model.project_data_copy_batch import ProjectDataCopyBatch
from libica.openapi.v2.model.temp_credentials import TempCredentials
from libica.openapi.v2.model.upload import Upload

# Local imports
from ...enums import DataType, ProjectDataSortParameters, ProjectDataStatusValues
from ...utils.configuration import get_icav2_configuration, logger
from ...utils.globals import LIBICAV2_DEFAULT_PAGE_SIZE
from ...utils.miscell import is_uuid_format


def get_project_data_file_id_from_project_id_and_path(
        project_id: str,
        file_path: Path,
        create_file_if_not_found: bool = False
) -> str:
    """
    Given a project id, parent folder path and file_name, return the file id
    If the file is not found, and create_file_if_not_found is True, create the file

    :param project_id:  The project id
    :param file_path:  The path to the file
    :param create_file_if_not_found:  Create the file object if it does not exist

    :return: The file id

    :raises: FileNotFoundError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import (
            create_download_urls, get_project_folder_id_from_project_id_and_path
        )
        from wrapica.libica_models import DataUrlWithPath

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        file_id: str = get_project_file_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            file_path=Path("/path/to/file.txt")
        )

        download_urls: List[DataUrlWithPath] = create_download_urls(
            project_id="proj.abcdef1234567890",
            folder_id=project_folder_obj.data.id,
            recursive=True
        )

        for download_url in download_urls:
            print(download_url.url)

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
            project_id=project_id,
            parent_folder_path=parent_folder_path,
            filename=filename,
            filename_match_mode="EXACT",
            file_path_match_mode="FULL_CASE_INSENSITIVE",
            type="FILE"
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
    except StopIteration:
        logger.error("Could not find file id for file: %s\n" % file_path)
        raise FileNotFoundError

    return file_id.data.id


def create_data_in_project(
        project_id: str,
        parent_folder_path: Path,
        data_name: str,
        data_type: DataType
) -> ProjectData:
    """
    Create a data object in a project context

    :param project_id: The project ID
    :param parent_folder_path: The parent folder path of where the data object needs to be created
    :param data_name:  The name of the file or folder
    :param data_type:  One of DataType.FILE or DataType.FOLDER

    :return: The newly created project data object
    :rtype: List[`ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import (
            create_data_in_project, create_data_in_project,
        )
        from wrapica.enums import DataType
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_obj: ProjectData = create_data_in_project(
            project_id="abcd-1234-efab-5678",
            parent_folder_path=Path("/path/to/folder/"),
            data_name="file.txt",
            data_type=DataType.FILE
        )
    """

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
        api_response: ProjectData = api_instance.create_data_in_project(
            project_id=project_id,
            create_data=CreateData(
                name=data_name,
                folder_path=parent_folder_path,
                data_type=DataType(data_type).value,
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_project_data: %s\n" % e)
        raise ApiException

    # Return the folder id
    return api_response


def create_file_in_project(
        project_id: str,
        file_path: Path,
) -> ProjectData:
    """
    Create a file in a project

    :param project_id: The project id to create the file in
    :param file_path: The path to the file

    :return: The newly created file

    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import create_file_in_project
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_obj: ProjectData = create_file_in_project(
            project_id="abcd-1234-efab-5678",
            file_path=Path("/path/to/file.txt")
        )
    """
    return create_data_in_project(
        project_id=project_id,
        parent_folder_path=file_path.parent,
        data_name=file_path.name,
        data_type=DataType.FILE
    )


def create_folder_in_project(
        project_id: str,
        folder_path: Path,
) -> ProjectData:
    """
    Create a folder in a project

    :param project_id:  The project ID
    :param folder_path:  The path to the folder to create

    :return: The newly created folder project data object
    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import create_folder_in_project
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_obj: ProjectData = create_folder_in_project(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/new/")
        )

    """
    return create_data_in_project(
        project_id=project_id,
        parent_folder_path=folder_path.parent,
        data_name=folder_path.name,
        data_type=DataType.FOLDER
    )


def get_project_data_folder_id_from_project_id_and_path(
        project_id: str,
        folder_path: Path,
        create_folder_if_not_found: bool = False
) -> str:
    """
    Given a project_id and a path, return the folder_id.

    Note that given the folder_path is a Path object (which do not end in /),
    we need to append a '/' to the end of the path before calling the API
    In the case that the folder_path is the root folder, we ensure that only a single '/' is provided.

    :param project_id:  The project id to search in
    :param folder_path:  The path to the folder
    :param create_folder_if_not_found:  If folder does not exist in project, do we want to create it?

    :return: The folder id

    :raises: NotADirectoryError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_folder_id_from_project_id_and_path

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        folder_id: str = get_project_folder_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/")
        )
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
            project_id=project_id,
            parent_folder_path=parent_folder_path,
            filename=folder_name,
            filename_match_mode="EXACT",
            file_path_match_mode="FULL_CASE_INSENSITIVE",
            type="FOLDER"
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
        project_id: str,
        data_path: Path,
        data_type: DataType,
        create_data_if_not_found: bool = False
) -> str:
    """
    Given a project_id and a path, return the data_id, where DATA_TYPE is one of FILE or FOLDER
    Should call the underlying get_data_id_from_project_id_and_path function split by data type

    :param project_id: The project context the data exists in
    :param data_path: The path to the data in the project
    :param data_type: The data_type, one of DataType.FILE, DataType.FOLDER
    :param create_data_if_not_found:

    :raises: FileNotFoundError, NotADirectoryError, ApiException

    :return: The data id

    :note:
      Use get_file_id_from_project_id_and_path or get_folder_id_from_project_id_and_path instead if data_type is known

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_id_from_project_id_and_path
        from wrapica.enums import DataType

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        # Get a folder
        data_id: str = get_project_data_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/folder/"),
            data_type=DataType.FOLDER
        )

        # Get a file
        data_id: str = get_project_data_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/file.txt"),
            data_type=DataType.FILE
        )
    """
    if data_type == DataType.FOLDER:
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
        project_id: str,
        data_id: str
) -> ProjectData:
    """
    Given a project_id and a data_id, return the data object

    :param project_id: The project id to search in
    :param data_id: The data id

    :return: The project data object
    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import get_project_data_obj_by_id
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_obj: ProjectData = get_project_data_obj_by_id(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )
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
            project_id=project_id,
            data_id=data_id
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->get_project_data_list: %s\n" % e)
        raise ApiException

    return data_obj


def get_project_data_obj_from_project_id_and_path(
        project_id: str,
        data_path: Path,
        data_type: DataType,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Given a project_id and a path, return the data object, where DATA_TYPE is one of FILE or FOLDER
    Will call the get_project_data_id and then call get_project_data_obj_by_id

    :param project_id: The project id to search in
    :param data_path: The path to the data in the project
    :param data_type: The data_type, one of DataType.FILE, DataType.FOLDER
    :param create_data_if_not_found: If the data is not found, and create_data_if_not_found is True, create the data

    :return: The project data object
    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: FileNotFoundError, NotADirectoryError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_obj_from_project_id_and_path
        from wrapica.enums import DataType
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        # Get a folder project data object
        project_folder_data_obj: ProjectData = get_project_data_obj_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/folder/"),
            data_type=DataType.FOLDER
        )

        # Get a file project data object
        project_file_data_obj: ProjectData = get_project_data_obj_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/file.txt"),
            data_type=DataType.FILE
        )

        print(project_folder_data_obj.data.id)
        # fol.abcdef1234567890

        print(project_file_data_obj.data.id)
        # fil.abcdef1234567890
    """
    # Collect the data id, either fol.id or fil.id
    project_data_id: str = get_project_data_id_from_project_id_and_path(
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
        project_id: str,
        data_id: str
) -> Path:
    """
    Given a project id and data id, return the path of the data

    :param project_id: The project id to search in
    :param data_id: The data id

    :return: The path of the data
    :rtype: Path

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import get_project_data_path_by_id

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_path: Path = get_project_data_path_by_id(
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
        project_id: str,
        parent_folder_id: Optional[str] = None,
        parent_folder_path: Optional[Path] = None,
        file_name: Optional[Union[str, List[str]]] = None,
        status: Optional[Union[ProjectDataStatusValues, List[ProjectDataStatusValues]]] = None,
        data_type: Optional[DataType] = None,
        creation_date_after: Optional[datetime] = None,
        creation_date_before: Optional[datetime] = None,
        status_date_after: Optional[datetime] = None,
        status_date_before: Optional[datetime] = None,
        sort: Optional[Union[ProjectDataSortParameters, List[ProjectDataSortParameters]]] = ""
) -> List[ProjectData]:
    """
    Given a project id and parent folder id or path,
    return a list of data objects that are directly under that folder

    :param project_id: The project id to search in
    :param parent_folder_path: The path to the parent folder (can use parent_folder_id instead)
    :param parent_folder_id: The parent folder id (can use parent_folder_path instead)
    :param file_name: The name of the file or directory to look for, can also be a list of names, may also use * as a wildcard
    :param status: The status of the data, one of ProjectDataStatusValues
    :param data_type: The type of the data, one of DataType.FILE, DataType.FOLDER
    :param creation_date_after: Return only data created after this date
    :param creation_date_before: Return only data created before this date
    :param status_date_after: Return only data with status date after this date
    :param status_date_before: Return only data with status date before this date
    :param sort: The sort order, one or more of ProjectDataSortParameters (Use '-' prefix to sort in descending order)
      * timeCreated - Sort by time created
      * timeModified - Sort by time modified
      * name - Sort by name
      * path - Sort by path
      * fileSizeInBytes - Sort by file size in bytes
      * status - Sort by status
      * format - Sort by format
      * dataType - Sort by data type
      * willBeArchivedAt - Sort by when the data will be archived
      * willBeDeletedAt - Sort by when the data will be deleted

    :return: List of data objects
    :rtype: List[`ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_]

    :raises: AssertionError, ApiException, ValueError

    :Examples:

    .. code-block:: python
        :linenos:

        from pathlib import Path
        from wrapica.project_data import list_project_data_non_recursively
        from wrapica.libica_models import ProjectData, ProjectDataSortParameters
        from wrapica.enums import ProjectDataStatusValues, DataType

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_list: List[ProjectData] = list_project_data_non_recursively(
            project_id="abcd-1234-efab-5678",
            parent_folder_path=Path("/path/to/folder/"),
            file_name="file.txt",
            status=ProjectDataStatusValues.COMPLETED,
            data_type=DataType.FILE,
            creation_date_after=datetime(2021, 1, 1),
            creation_date_before=datetime(2021, 12, 31),
            status_date_after=datetime(2021, 1, 1),
            status_date_before=datetime(2021, 12, 31),
            sort=ProjectDataSortParameters.TIME_CREATED
        )

        for project_data in project_data_list:
            print(project_data.data.details.name)

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
        if isinstance(status, ProjectDataStatusValues):
            status = [status]
        elif isinstance(status, str):
            status = [ProjectDataStatusValues(status)]
        status = list(
            map(
                lambda status_iter: status_iter.value,
                status
            )
        )

    # Check data_type
    if data_type is not None:
        if isinstance(data_type, (DataType, str)):
            data_type = DataType(data_type).value

    # Check sort
    if sort == "":
        sort = None

    if sort is not None:
        if isinstance(sort, ProjectDataSortParameters):
            sort = [sort]
        elif isinstance(sort, str):
            sort = [ProjectDataSortParameters(sort)]
        # Complete a comma join of the sort parameters
        sort = ", ".join(
            list(
                map(
                    lambda sort_iter: sort_iter.value,
                    sort
                )
            )
        )

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
    data_ids: List[ProjectData] = []

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
                            "project_id": project_id,
                            "parent_folder_id": parent_folder_id,
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
        data_ids.extend(api_response.items)

        if sort is not None:
            # Check page offset and page size against total item count
            if page_offset + page_size > api_response.total_item_count:
                break
            page_offset += page_size
        else:
            # Check if there is a next page
            if api_response.next_page_token is None or api_response.next_page_token == "":
                break
            page_token = api_response.next_page_token

    return data_ids


def find_project_data_recursively(
        project_id: str,
        parent_folder_id: Optional[str] = None,
        parent_folder_path: Optional[Path] = None,
        name: Optional[str] = None,
        data_type: Optional[DataType] = None,
        min_depth: Optional[int] = None,
        max_depth: Optional[int] = None
) -> List[ProjectData]:
    """
    Given a project_id, a parent_folder_id, a data_name and a data_type, return a list of data objects
    This is a slow exercise and should only be used if the max_depth is low and the total number of items in the
    directory is very high

    :param project_id: The project id to search in
    :param parent_folder_id: The parent folder id (alternative to parent_folder_path)
    :param parent_folder_path: The path to the parent folder (alternative to parent_folder_id)
    :param name: The name of the file or directory to look for, may also use * as a wildcard
    :param data_type: The type of the data, one of DataType.FILE, DataType.FOLDER
    :param min_depth: The minimum depth to search
    :param max_depth: The maximum depth to search

    :return: List of data objects
    :rtype: List[`ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_]

    :raises: ApiException, AssertionError, ValueError

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import find_project_data_recursively
        from wrapica.enums import DataType
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_list: List[ProjectData] = find_project_data_recursively(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            name="file.txt",
            data_type=DataType.FILE,
            min_depth=1,
            max_depth=3
        )

        for project_data in project_data_list:
            print(project_data.data.details.name)

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

    if name is not None:
        name = '.*'
    name_regex_obj = re.compile(name)

    # Get top level items
    data_items: List[ProjectData] = list_project_data_non_recursively(
        project_id=project_id,
        parent_folder_id=parent_folder_id,
        parent_folder_path=parent_folder_path,
        data_type=data_type
    )

    # Check if we can pull out any items in the top directory
    if min_depth is None or min_depth <= 1:
        for data_item in data_items:
            data_item_match = name_regex_obj.match(data_item.data.details.name)
            if data_type is not None and not DataType[data_item.data.details.data_type] == data_type:
                continue
            if data_item_match is not None:
                matched_data_items.append(data_item)

    # Otherwise look recursively
    if max_depth is None or not max_depth <= 1:
        # Listing sub folders
        subfolders = list(
            filter(
                lambda x: x.data.details.data_type == "FOLDER",
                data_items
            )
        )
        for subfolder in subfolders:
            matched_data_items.extend(
                find_project_data_recursively(
                    project_id=project_id,
                    parent_folder_id=subfolder.data.id,
                    parent_folder_path=subfolder.data.details.path,
                    name=name,
                    data_type=data_type,
                    min_depth=min_depth - 1 if min_depth is not None else None,
                    max_depth=max_depth - 1 if max_depth is not None else None
                )
            )

        return matched_data_items


def find_project_data_bulk(
        project_id: str,
        parent_folder_id: Optional[str] = None,
        parent_folder_path: Optional[Path] = None,
        data_type: Optional[DataType] = None
) -> List[ProjectData]:
    """
    Given a project_id and a parent_folder_id, return a list of all data objects in the folder (recursively)

    :param project_id: The project id to search in
    :param parent_folder_id: The parent folder id (alternative to parent_folder_path)
    :param parent_folder_path: The path to the parent folder (alternative to parent_folder_id)
    :param data_type: The type of the data, one of DataType.FILE, DataType.FOLDER

    :return: List of data objects
    :rtype: List[`ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_]

    :raises: ApiException, AssertionError

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import find_project_data_bulk
        from wrapica.enums import DataType
        from wrapica.libica_models import ProjectData

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_list: List[ProjectData] = find_project_data_bulk(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            data_type=DataType.FILE
        )

        for project_data in project_data_list:
            print(project_data.data.details.name)
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
                project_id=project_id,
                file_path=[parent_folder_path],
                file_path_match_mode="STARTS_WITH_CASE_INSENSITIVE",
                page_size=str(page_size),
                page_token=page_token,
                type=data_type.value
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
        project_id: str,
        file_id: str
) -> str:
    """
    Given a project_id and a data_id, create a presigned url for a file
    in project-pipeline, or view a file in project-data
    Create download URL

    :param project_id: The owning project id
    :param file_id: The id of the file

    :return: The download URL string
    :rtype: str

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import create_download_url

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        download_url: str = create_download_url(
            project_id="proj.abcdef1234567890",
            file_id="fil.abcdef1234567890"
        )

        print(download_url)
        # https://s3.amazonaws.com/umccr-illumina-prod/abcd-1234-efab-5678/abcdef1234567890

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
                project_id,
                file_id
            )
        except ApiException as e:
            logger.error("Exception when calling ProjectDataApi->create_download_url_for_data: %s\n" % e)
            raise ApiException

    return api_response.get("url")


def create_download_urls(
        project_id: str,
        folder_id: str,
        recursive: bool = False
) -> List[DataUrlWithPath]:
    """
    Given a project data folder return a list where each item is an object with the following attributes

    :param project_id: The owning project id
    :param folder_id:  The id of the folder
    :param recursive:  Whether to provide download urls recursively

    :return: List of download urls
    :rtype: List[`DataUrlWithPath <https://umccr-illumina.github.io/libica/openapi/v2/docs/DataUrlWithPathList/>`_]

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import (
            create_download_urls, get_project_folder_id_from_project_id_and_path
        )
        from wrapica.libica_models import ProjectData, DataUrlWithPath

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_folder_obj: ProjectData = get_project_folder_id_from_project_id_and_path(
            project_id="abcd-1234-efab-5678",
            folder_path=Path("/path/to/folder/")
        )

        download_urls: List[DataUrlWithPath] = create_download_urls(
            project_id="proj.abcdef1234567890",
            folder_id=project_folder_obj.data.id,
            recursive=True
        )

        for download_url in download_urls:
            print(download_url.url)
    """

    if recursive:
        project_data_list = find_project_data_bulk(
            project_id=project_id,
            parent_folder_id=folder_id,
            data_type=DataType.FILE
        )
    else:
        project_data_list = list_project_data_non_recursively(
            project_id=project_id,
            parent_folder_id=folder_id,
            data_type=DataType.FILE
        )

    # Set data paths
    data_id_paths_list = DataIdOrPathList(
        data_ids=list(
            map(
                lambda project_file_iter: project_file_iter.data.id,
                project_data_list
            )
        )
    )

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve download URLs for the data.
        api_response = api_instance.create_download_urls_for_data(project_id, data_id_paths_list)
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
    Given an ICAv2 URI, return a project data object

    :param data_uri: The icav2 uri to convert to a data object
    :param create_data_if_not_found:  If the data is not found, and create_data_if_not_found is True, create the data

    :return: libica v2 Project Data Object
    :rtype: `Project Data <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import convert_icav2_uri_to_data_obj, ProjectData

        project_data_object: ProjectData = convert_icav2_uri_to_data_obj(
            "icav2://project-name/path/to/data/"
        )

        print(project_data_object.data.id)
        # file.abcdef1234567890
    """
    # Import other functions locally to avoid circular imports
    from ...project import get_project_id_from_project_name

    data_uri_obj = urlparse(data_uri)

    # Set data type
    if data_uri_obj.path.endswith("/"):
        data_type = DataType.FOLDER
    else:
        data_type = DataType.FILE

    # Check if the project is in project id format
    if is_uuid_format(data_uri_obj.netloc):
        project_id = data_uri_obj.netloc
    else:
        project_id = get_project_id_from_project_name(data_uri_obj.netloc)

    # Return the data object
    return get_project_data_obj_from_project_id_and_path(
        project_id=project_id,
        data_path=Path(data_uri_obj.path),
        data_type=data_type,
        create_data_if_not_found=create_data_if_not_found
    )


def convert_project_data_obj_to_icav2_uri(
    project_data: ProjectData
) -> str:
    """
    Return the file object as an icav2:// uri

    :param project_data: The ProjectData object

    :return: The icav2:// uri string
    """
    return str(
        urlunparse((
            "icav2",
            project_data.project_id,
            project_data.data.details.path.rstrip("/") + ("/" if project_data.data.details.data_type == "FOLDER" else ""),
            None, None, None
        ))
    )


def convert_project_id_and_data_path_to_icav2_uri(
    project_id: str,
    data_path: Path,
    data_type: DataType
) -> str:
    """
    Given a project_id and a data_id, return the icav2:// uri

    Unlike convert_project_data_obj_to_icav2_uri, this does not require the path to exist.

    If the data_type is DataType.FOLDER, the path should end with a forward slash.

    :param project_id: The project id to search in
    :param data_path: The path to the data in the project
    :param data_type: The data_type, one of DataType.FILE, DataType.FOLDER

    :return: The icav2:// uri string
    :rtype: str

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_data import convert_project_id_and_data_path_to_icav2_uri
        from wrapica.enums import DataType

        icav2_uri: str = convert_project_id_and_data_path_to_icav2_uri(
            project_id="abcd-1234-efab-5678",
            data_path=Path("/path/to/folder/"),
            data_type=DataType.FOLDER
        )
    """

    return str(
        urlunparse((
            "icav2",
            project_id,
            str(data_path) + ("/" if data_type == DataType.FOLDER else ""),
            None, None, None
        ))
    )


def get_aws_credentials_access_for_project_folder(
        project_id: str,
        folder_id: Optional[str],
        folder_path: Optional[Path]
) -> AwsTempCredentials:
    """
    Given a project_id and a folder_id or folder_path, collect the AWS Access Credentials for downloading this data.

    :param project_id: The project id of the data
    :param folder_id: The folder id (alternative to folder_path)
    :param folder_path: The folder path (alternative to folder_id)

    :return: An object with the following attributes:
      * access_key
      * secret_key
      * session_token
      * region
      * bucket
      * object_prefix
      * server_side_encryption_algorithm
      * server_side_encryption_key

    :rtype: `AwsTempCredentials <https://umccr-illumina.github.io/libica/openapi/v2/docs/AwsTempCredentials/>`_

    :raises: AssertionError, ApiException, ValueError

    :Examples:

    .. code-block:: python
        :linenos:

        import subprocess
        from wrapica.project_data import get_aws_credentials_access_for_project_folder
        from wrapica.libica_models import AwsTempCredentials

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        aws_temp_credentials: AwsTempCredentials = get_aws_credentials_access_for_project_folder(
            project_id="proj.abcdef1234567890",
            folder_path=Path("/path/to/folder/")
        )

        local_path = Path("/path/to/local/folder/")

        subprocess.run(
          [
            "aws", "s3", "sync",
            # Can add sync parameters here like --dryrun or --exclude / --include
            "s3://{}/{}".format(aws_temp_credentials.bucket, aws_temp_credentials.object_prefix),
            str(local_path)
          ],
          env={
            "AWS_ACCESS_KEY_ID": aws_temp_credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": aws_temp_credentials.secret_key,
            "AWS_SESSION_TOKEN": aws_temp_credentials.session_token,
            "AWS_REGION": aws_temp_credentials.region
          }
        )
    """
    # Check one of parent_folder_id and parent_folder_path is specified
    if folder_id is None and folder_path is None:
        logger.error("Must specify one of parent_folder_id and parent_folder_path")
        raise AssertionError
    elif folder_id is not None and folder_path is not None:
        logger.error("Must specify only one of parent_folder_id and parent_folder_path")
        raise AssertionError

    if folder_id is None:
        folder_id = get_project_data_folder_id_from_project_id_and_path(
            project_id=project_id,
            folder_path=folder_path
        )

    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    create_temporary_credentials = CreateTemporaryCredentials()

    # example passing only required values which don't have defaults set
    try:
        # Retrieve temporary credentials for this data.
        api_response: TempCredentials = api_instance.create_temporary_credentials_for_data(
            project_id, folder_id, create_temporary_credentials=create_temporary_credentials
        )
    except ApiException as e:
        logger.warning("Exception when calling ProjectDataApi->create_temporary_credentials_for_data: %s\n" % e)
        raise ValueError

    return api_response.aws_temp_credentials


def is_folder_id_format(folder_id_str: str) -> bool:
    """
    Does this string look like a folder id?

    :param folder_id_str: The string to check

    :return: True if the string looks like a folder id
    :rtype: bool

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import is_folder_id_format

        print(is_folder_id_format("fol.abcdef1234567890"))
        # True
    """
    return re.match("fol.[0-9a-f]{32}", folder_id_str) is not None


def is_file_id_format(file_id_str: str) -> bool:
    """
    Does this string look like a folder id?

    :param file_id_str: The string to check

    :return: True if the string looks like a file id
    :rtype: bool

    :Examples:

    .. code-block:: python
    :linenos:

        from wrapica.project_data import is_file_id_format

        print(is_file_id_format("fil.abcdef1234567890"))
        # True

    """
    return re.match("fil.[0-9a-f]{32}", file_id_str) is not None


def is_data_id(data_id: str) -> bool:
    """
    Check if data id is a data id

    :param data_id: The data id to check

    :return: True if the data id is a data id

    :rtype: bool

    :Examples:

    .. code-block:: python

    from wrapica.project_data import is_data_id

    print(is_data_id("fil.abcdef1234567890"))
    # True
    """
    return is_file_id_format(data_id) or is_folder_id_format(data_id)


def presign_cwl_directory(
        project_id: str,
        data_id: str
) -> List[
    Union[
        Dict[str, Union[Union[dict, str], Any]],
        Dict[str, Union[str, Any]]
    ]
]:
    """
    Given a CWL directory object, presign all files in the directory recursively, and return the list of presigned url

    :param project_id: The project id to search in
    :param data_id: The data id

    :return: The CWL input json Directory object where each file in the listing has a presigned url for a location attributes
    :rtype: List[Dict[str, Union[Union[dict, str], Any]]]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import presign_cwl_directory

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id
        # Use wrapica.project_data.get_folder_id_from_project_id_and_path
        # If you need to convert a folder path to a folder id

        cwl_directory: List[Dict[str, Union[Union[dict, str], Any]]] = presign_cwl_directory(
            project_id="abcd-1234-efab-5678",
            data_id="fol.abcdef1234567890"
        )

        print(cwl_directory)
        # [
        #   {
        #     "class": "Directory",
        #     "basename": "folder",
        #     "listing": [
        #       {
        #         "class": "File",
        #         "basename": "file.txt",
        #         "location": "https://s3.amazonaws.com/umccr-illumina-prod/abcd-1234-efab-5678/abcdef1234567890"
        #       }
        #     ]
        #   }
        # ]
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
        data_type: str = file_item_obj.get("data").get("details").get('data_type')  # One of FILE | FOLDER
        data_id = file_item_obj.get("data").get("id")
        basename = file_item_obj.get("data").get("details").get("name")
        if data_type == "FOLDER":
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
    project_id: str,
    data_id: str
) -> Tuple[
    # External data mounts
    List[AnalysisInputExternalData],
        # Dict listing
    List[Dict]
]:
    """
    Given a cwl directory with a listing attribute, presign all files in the directory recursively, and return the
    list of presigned url mount objects and the cwl directory listing object

    :param project_id: The project id to search in
    :param data_id: The data id

    :return: external_data_mounts, cwl_item_objs
    :rtype: Tuple[List[`AnalysisInputExternalData <https://umccr-illumina.github.io/libica/openapi/v2/docs/AnalysisInputExternalData/>`_], List[Dict]]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.project_data import presign_cwl_directory_with_external_data_mounts

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id
        # Use wrapica.project_data.get_folder_id_from_project_id_and_path
        # If you need to convert a folder path to a folder id

        external_data_mounts, cwl_item_objs = presign_cwl_directory_with_external_data_mounts(
            project_id="abcd-1234-efab-5678",
            data_id="fol.abcdef1234567890"
        )

        print(external_data_mounts)
        # [
        #   {
        #     "url": "https://s3.amazonaws.com/umccr-illumina-prod/abcd-1234-efab-5678/abcdef1234567890",
        #     "type": "FILE",
        #     "mount_path": "abcd-1234-efab-5678/abcdef1234567890/file.txt"
        #   }
        # ]

        print(cwl_item_objs)
        # [
        #   {
        #     "class": "Directory",
        #     "basename": "folder",
        #     "listing": [
        #       {
        #         "class": "File",
        #         "basename": "file.txt",
        #         "location": "abcd-1234-efab-5678/abcdef1234567890/file.txt"
        #       }
        #     ]
        #   }
        # ]

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
        data_type: str = file_item_obj.get("data").get("details").get('data_type')  # One of FILE | FOLDER
        data_id = file_item_obj.get("data").get("id")
        basename = file_item_obj.get("data").get("details").get("name")
        if data_type == "FOLDER":
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
                    mount_path=mount_path
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
    project_id: str,
    data_id: str,
    output_path: Union[Path, TextIOWrapper]
):
    """
    Write icav2 file contents to a path

    :param project_id:
    :param data_id:
    :param output_path:
    :return:
    """
    if isinstance(output_path, Path):
        # Ensure parent directory exists
        if not output_path.parent.exists():
            logger.error(f"Could not write to output path {output_path} as the parent directory does not exist")
            raise NotADirectoryError

    # Get the presigned url
    presigned_url = create_download_url(project_id, data_id)

    # Get the file contents with the requests package
    r = requests.get(presigned_url)

    if isinstance(output_path, Path):
        # Write the file contents to the output path
        with open(output_path, "wb") as f:
            f.write(r.content)
    else:
        # Write the file contents to the output path
        output_path.write(r.content.decode())


def read_icav2_file_contents_to_string(
    project_id: str,
    data_id: str
) -> str:
    """

    Stream down the icav2 file contents and return as a string

    :param project_id: The project id
    :param data_id: The data id

    :return: The file contents as a string
    :rtype: str

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:
        from wrapica.project_data import read_icav2_file_contents_to_string

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        file_contents: str = read_icav2_file_contents_to_string(
            project_id="abcd-1234-efab-5678",
            data_id="fil.abcdef1234567890"
        )

        print(file_contents)
        # this is the file contents
    """

    with NamedTemporaryFile() as temp_file_h:
        read_icav2_file_contents(
            project_id=project_id,
            data_id=data_id,
            output_path=Path(temp_file_h.name)
        )

        with open(temp_file_h.name, "r") as f:
            return f.read()


def get_project_data_upload_url(
    project_id: str,
    data_id: str
):
    # The client must configure the authentication and authorization parameters
    # in accordance with the API server security policy.

    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectDataApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve an upload URL for this data.
        api_response: Upload = api_instance.create_upload_url_for_data(project_id, data_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->create_upload_url_for_data: %s\n" % e)
        raise ApiException

    return api_response.url


def write_icav2_file_contents(
    project_id: str,
    data_path: Path,
    file_stream_or_path: Union[Path, TextIOWrapper]
):
    """
    Write icav2 file contents to a path

    :param project_id:
    :param data_path:
    :param file_stream_or_path:
    :return:
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
    Useful for collecting a file object from an analysis output object

    :param file_name: The name of the file to get
    :param project_data_list: The list of project data objects to search through

    :return: The file object
    :rtype: `ProjectData <https://umccr-illumina.github.io/libica/openapi/v2/docs/ProjectData/>`_

    :raises: ValueError

    :Examples:

    .. code-block:: python

        from wrapica.project_data import get_file_by_file_name_from_project_data_list

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

        project_data_list: List[ProjectData] = find_project_data_bulk(
            project_id="abcd-1234-efab-5678",
            parent_folder_id="fol.abcdef1234567890",
            data_type=DataType.FILE
        )

        file_obj: ProjectData = get_file_by_file_name_from_project_data_list(
            file_name="file.txt",
            project_data_list=project_data_list
        )
    """

    # Find the first file with this name
    try:
        return next(
            filter(
                lambda file_iter: (
                    file_iter.data.details.name == file_name and
                    file_iter.data.details.data_type == DataType.FILE.value
                ),
                project_data_list
            )
        )
    except StopIteration:
        logger.error(f"Could not get file {file_name} from analysis output")
        raise ValueError


def project_data_copy_batch_handler(
    source_data_ids: List[str],
    destination_project_id: str,
    destination_folder_path: Path
) -> Job:
    """
    Copy a batch of files from one project to another
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
            project_id=destination_project_id,
            create_project_data_copy_batch=CreateProjectDataCopyBatch(
                items=list(
                    map(
                        lambda source_data_id_iter: CreateProjectDataCopyBatchItem(
                            data_id=source_data_id_iter
                        ),
                        source_data_ids
                    )
                ),
                destination_folder_id=get_project_data_folder_id_from_project_id_and_path(
                    project_id=destination_project_id,
                    folder_path=destination_folder_path,
                    create_folder_if_not_found=True
                ),
                copy_user_tags=True,
                copy_technical_tags=True,
                copy_instrument_info=True,
                action_on_exist="SKIP"
            )
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->copy_project_data_batch: %s\n" % e)
        raise ApiException

    # Return the job ID for the project data copy batch
    return api_response.job


def delete_project_data(project_id: str, data_id: str):
    """
    Delete a project data item using the projectData:delete endpoint

    :param project_id: The project id the data belongs to
    :param data_id: The data id we want to delete

    :return: None
    :rtype: None

    :raises: ValueError, ApiException

    :Examples:

    .. code-block:: python

        from wrapica.project_data import delete_project_data

        # Use wrapica.project.get_project_id_from_project_name
        # If you need to convert a project_name to a project_id

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
        api_instance.delete_data(project_id, data_id)
    except ApiException as e:
        logger.error("Exception when calling ProjectDataApi->delete_data: %s\n" % e)
        raise ApiException
