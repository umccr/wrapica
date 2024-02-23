#!/usr/bin/env python3

"""
List of available functions:

"""

# Standard imports
import re
from pathlib import Path
from typing import Dict, List, Union, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse

# Libica imports
from libica.openapi.v2 import ApiClient, ApiException
from libica.openapi.v2.api.project_data_api import ProjectDataApi
from libica.openapi.v2.model.analysis_input_external_data import AnalysisInputExternalData
from libica.openapi.v2.model.create_data import CreateData
from libica.openapi.v2.model.create_temporary_credentials import CreateTemporaryCredentials
from libica.openapi.v2.model.data_id_or_path_list import DataIdOrPathList
from libica.openapi.v2.model.data_url_with_path import DataUrlWithPath
from libica.openapi.v2.model.download import Download
from libica.openapi.v2.model.project_data import ProjectData
from libica.openapi.v2.model.temp_credentials import TempCredentials

# Local imports
from ...utils.enums import DataType, ProjectDataSortParameters, ProjectDataStatusValues
from ...utils.configuration import get_icav2_configuration, logger
from ...utils.globals import LIBICAV2_DEFAULT_PAGE_SIZE
from ...utils.miscell import is_uuid_format


def get_project_file_id_from_project_id_and_path(
        project_id: str,
        file_path: Path,
        create_file_if_not_found: bool = False
) -> str:
    """
    Given a project id, parent folder path and file_name, return the file id
    If the file is not found, and create_file_if_not_found is True, create the file
    :param project_id: 
    :param file_path:
    :param create_file_if_not_found: 
    :return: 
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
                parent_folder_path=Path(parent_folder_path),
                file_name=file_path.name
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
    Create a folder in a project
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
    parent_folder_path: Path,
    file_name: str
) -> ProjectData:
    """
    Create a file in a project
    """
    return create_data_in_project(
        project_id=project_id,
        parent_folder_path=parent_folder_path,
        data_name=file_name,
        data_type=DataType.FILE
    )


def create_folder_in_project(
    project_id: str,
    parent_folder_path: Path,
    folder_name: str
) -> ProjectData:
    """
    Create a folder in a project
    """
    return create_data_in_project(
        project_id=project_id,
        parent_folder_path=parent_folder_path,
        data_name=folder_name,
        data_type=DataType.FOLDER
    )


def get_project_folder_id_from_project_id_and_path(
    project_id: str,
    folder_path: Path,
    create_folder_if_not_found: bool = False
) -> str:
    """
    Given a project_id and a path, return the folder_id.

    Note that given the folder_path is a Path object (which do not end in /),
    we need to append a '/' to the end of the path before calling the API
    In the case that the folder_path is the root folder, we ensure that only a single '/' is provided.

    :param project_id:
    :param folder_path:
    :param create_folder_if_not_found:

    :return:
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
                parent_folder_path=folder_path.parent,
                folder_name=folder_path.name
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
    A few different methods, available, would look into bssh_manager implementation first
    Should call the underlying get_data_id_from_project_id_and_path function split by data type
    :return:
    """
    if data_type == DataType.FOLDER:
        return get_project_folder_id_from_project_id_and_path(
            project_id=project_id,
            folder_path=data_path,
            create_folder_if_not_found=create_data_if_not_found
        )
    else:
        return get_project_file_id_from_project_id_and_path(
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

    :param project_id:
    :param data_id:

    :return:
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
    :return:
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
    :param project_id:
    :param data_id:
    :return:
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
    Given a project id and parent folder id or path, return a list of data objects that are directly under that folder
    :param parent_folder_path:
    :param project_id:
    :param parent_folder_id:
    :param file_name:
    :param status:
    :param data_type:
    :param creation_date_after:
    :param creation_date_before:
    :param status_date_after:
    :param status_date_before:
    :param sort:
    :return:
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
                project_id=project_id,
                parent_folder_ids=parent_folder_id,
                parent_folder_path=parent_folder_path,
                page_size=str(page_size),
                page_offset=str(page_offset),
                page_token=page_token,
                file_name=file_name,
                status=status,
                data_type=data_type,
                creation_date_after=creation_date_after,
                creation_date_before=creation_date_before,
                status_date_after=status_date_after,
                status_date_before=status_date_before,
                sort=sort
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
            if api_response.next_page_token is None:
                break
            page_token = api_response.next_page_token

    return data_ids


def find_project_data_recursively(
    project_id: str,
    parent_folder_id: str,
    parent_folder_path: Path,
    name: str,
    data_type: DataType,
    min_depth: Optional[int] = None,
    max_depth: Optional[int] = None
) -> List[ProjectData]:
    """
    Given a project_id, a parent_folder_id, a data_name and a data_type, return a list of data objects
    This is a slow exercise and should only be used if the max_depth is low and the total number of items in the
    directory is very high
    :param project_id:
    :param parent_folder_id:
    :param parent_folder_path:
    :param name:
    :param data_type:
    :param min_depth:
    :param max_depth:
    :return:
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
            if data_type is not None and not data_item.data.details.data_type == data_type:
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
    :param project_id:
    :param parent_folder_id:
    :param parent_folder_path:
    :param data_type:
    :return:
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
    project_id: str,
    file_id: str
) -> str:
    """
    Given a project_id and a data_id, create a presigned url for a file
    in project-pipeline, or view a file in project-data
    Create download URL
    :param project_id:
    :param file_id:
    :return:
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
    * data_id: The data id
    * data_urn: The data urn
    * data_path: The project data path
    * url: The presigned url
    :param project_id:
    :param folder_id:
    :param recursive:
    :return:
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
    :param data_uri:
    :param create_data_if_not_found:
    :return: project_id, data_id, data_type
    """
    # Import other functions locally to avoid circular imports
    from ..project.project_functions import get_project_id_from_project_name

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


def get_aws_credentials_access_for_project_folder(
    project_id: str,
    folder_id: Optional[str],
    folder_path: Optional[Path]
) -> Dict:
    """
    Given a project_id and a folder_id or folder_path, collect the AWS Access Credentials for downloading this data.
    :param project_id:
    :param folder_id
    :param folder_path
    :return:
    """
    # Check one of parent_folder_id and parent_folder_path is specified
    if folder_id is None and folder_path is None:
        logger.error("Must specify one of parent_folder_id and parent_folder_path")
        raise AssertionError
    elif folder_id is not None and folder_path is not None:
        logger.error("Must specify only one of parent_folder_id and parent_folder_path")
        raise AssertionError

    if folder_id is None:
        folder_id = get_project_folder_id_from_project_id_and_path(
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
    :param folder_id_str:
    :return:
    """
    return re.match("fol.[0-9a-f]{32}", folder_id_str) is not None


def is_file_id_format(file_id_str: str) -> bool:
    """
    Does this string look like a folder id?
    :param file_id_str:
    :return:
    """
    return re.match("fil.[0-9a-f]{32}", file_id_str) is not None


def is_data_id(data_id):
    """
    Check if data id is a data id
    :param data_id:
    :return:
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
    Given a CWL directory object
    :param project_id:
    :param data_id:
    :return:
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
            listing, external_data_mounts_new = presign_cwl_directory_with_external_data_mounts(
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
                    type=DataType.FILE.value,
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
