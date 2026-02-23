#!/usr/bin/env python3

"""
Functions for storage configuration management
"""

# Standard imports
from os import environ
from pathlib import Path
from typing import List, Optional, Tuple, TypedDict, NotRequired, cast, Union
from urllib.parse import urlunparse, urlparse

from pydantic import UUID4
from ruamel.yaml import YAML

# Libica imports
from libica.openapi.v3 import ApiException, ApiClient, Project
from libica.openapi.v3.api.storage_configuration_api import StorageConfigurationApi
from libica.openapi.v3.models import (
    ProjectData,
)

# Local imports
from ...utils.configuration import get_icav2_configuration
from ...utils.logger import get_logger
from ...utils.globals import (
    S3_URI_SCHEME,
    ICAV2_URI_SCHEME,
    FOLDER_DATA_TYPE
)

# Get the logger
logger = get_logger()


class StorageConfigurationObjectModel(TypedDict):
    id: str
    bucketName: str
    keyPrefix: str


class ProjectToStorageMappingDictModel(TypedDict):
    id: str
    name: str
    storageConfigurationId: str
    prefix: NotRequired[str]


STORAGE_CONFIGURATION_OBJECT_LIST: Optional[List[StorageConfigurationObjectModel]] = None
STORAGE_CONFIGURATION_OBJECT_LIST_ENV_VAR = "ICAV2_STORAGE_CONFIGURATION_LIST_FILE"

PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST: Optional[List[ProjectToStorageMappingDictModel]] = None
PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_ENV_VAR = "ICAV2_PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_FILE"


def _get_storage_configuration_env_list() -> Optional[List[StorageConfigurationObjectModel]]:
    """
    Get the storage configuration list from an environment variable, this is expected to be a yaml file with the following format:
    - id: str
    - bucketName: str
    - keyPrefix: str

    :return: The storage configuration list, or None if the environment variable is not set or the file does not exist
    """
    if (
            environ.get(STORAGE_CONFIGURATION_OBJECT_LIST_ENV_VAR) is not None and
            Path(environ.get(STORAGE_CONFIGURATION_OBJECT_LIST_ENV_VAR)).is_file()
    ):
        yaml = YAML()

        with open(environ.get(STORAGE_CONFIGURATION_OBJECT_LIST_ENV_VAR)) as file_handle:
            # Load the YAML file
            return list(map(
                lambda item_iter_: cast(
                    StorageConfigurationObjectModel,
                    item_iter_
                ),
                yaml.load(file_handle)
            ))
    return None


def _get_storage_configuration_api_list() -> List[StorageConfigurationObjectModel]:
    """
    Get the storage configuration list using the ICA API, this is expected to be a list of storage configurations with the following format:
    - id: str
    - bucketName: str
    - keyPrefix: str

    :return: The storage configuration list
    """
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = StorageConfigurationApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve a list of storage configurations.
        api_response = api_instance.get_storage_configurations()
    except ApiException as e:
        logger.error("Exception when calling StorageConfigurationApi->get_storage_configurations: %s\n" % e)
        raise ApiException from e

    return list(map(
        lambda item_iter_: (
            cast(
                StorageConfigurationObjectModel,
                cast(
                    object,
                    {
                        "id": str(item_iter_.id),
                        "bucketName": item_iter_.storage_configuration_details.aws_s3.bucket_name,
                        "keyPrefix": str(Path(item_iter_.storage_configuration_details.aws_s3.key_prefix)) + "/"
                    }
                )
            )
        ),
        api_response.items
    ))


def _set_storage_configuration_list():
    """
    Set the storage configuration list,
    this will first attempt to get the list from an environment variable,
    if that is not set it will get the list from the API
    """
    global STORAGE_CONFIGURATION_OBJECT_LIST
    if STORAGE_CONFIGURATION_OBJECT_LIST is None:
        STORAGE_CONFIGURATION_OBJECT_LIST = _get_storage_configuration_env_list()

    if STORAGE_CONFIGURATION_OBJECT_LIST is not None:
        return

    # No environment variable set, so we need to get the list from the API
    # Which takes longer
    STORAGE_CONFIGURATION_OBJECT_LIST = _get_storage_configuration_api_list()


def get_storage_configuration_list() -> List[StorageConfigurationObjectModel]:
    """
    Get the storage configuration list, this will first check if the list has already been set,
    if not it will set the list using the _set_storage_configuration_list function

    The storage configuration list is expected to be a list of storage configurations with the following format:
    - id: str
    - bucketName: str
    - keyPrefix: str

    :return: The storage configuration list
    :rtype: `List[StorageConfigurationObjectModel]`
    """
    if STORAGE_CONFIGURATION_OBJECT_LIST is None:
        _set_storage_configuration_list()
        return get_storage_configuration_list()
    return STORAGE_CONFIGURATION_OBJECT_LIST


def _get_project_to_storage_configuration_env_mapping() -> Optional[List[ProjectToStorageMappingDictModel]]:
    """
    Get the project to storage configuration mapping list from an environment variable, this is expected to be a yaml file with the following format:
    - id: str
    - name: str
    - storageConfigurationId: str
    - prefix: Optional[str]

    :return: The project to storage configuration mapping list, or None if the environment variable is not set or the file does not exist
    :rtype: Optional[List[ProjectToStorageMappingDictModel]]
    """
    if (
            environ.get(PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_ENV_VAR) is not None and
            Path(environ.get(PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_ENV_VAR)).is_file()
    ):
        yaml = YAML()

        with open(environ.get(PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST_ENV_VAR)) as file_handle:
            # Load the YAML file
            return list(map(
                lambda item_iter_: cast(
                    ProjectToStorageMappingDictModel,
                    item_iter_
                ),
                yaml.load(file_handle)
            ))
    return None


def get_project_storage_configuration_prefix(
        project: Project
) -> Optional[str]:
    """
    Get the prefix used for a project with a self managed storage configuration,
    this is the subfolder of the storage configuration key prefix that is used for this project

    :param project: The project object
    :return: The prefix for this project, or None if there is no self managed storage configuration for this project
    :rtype: Optional[str]
    """
    # Check self managed storage configuration exists for this project, if not return None
    if project.self_managed_storage_configuration is None:
        return None

    # Get the storage configuration object
    try:
        storage_configuration_dict = next(filter(
            lambda storage_configuration_iter: (
                    storage_configuration_iter['id'] == str(project.self_managed_storage_configuration.id)
            ),
            get_storage_configuration_list()
        ))
    except StopIteration:
        return None

    # Get the project subfolder
    project_s3_uri = get_project_self_storage_configuration_subfolder(project.id)

    # Shouldn't get here
    if project_s3_uri is None:
        return None

    # Get the project subfolder relative to the storage configuration dict
    project_prefix = str(Path(urlparse(project_s3_uri).path.lstrip("/")).relative_to(Path(storage_configuration_dict['keyPrefix'])))

    if project_prefix == ".":
        return ""

    return project_prefix


def _get_project_to_storage_configuration_api_mapping() -> List[ProjectToStorageMappingDictModel]:
    """
    Get the project to storage configuration mapping list using the ICA API, this is expected to be a list of projects with the following format:
    - id: str
    - name: str
    - storageConfigurationId: str
    - prefix: Optional[str]

    :return: The project to storage configuration mapping list
    :rtype: `List[ProjectToStorageMappingDictModel]`
    """
    from wrapica.project import list_projects

    # Get the storage configuration list
    storage_configuration_list: List[StorageConfigurationObjectModel] = get_storage_configuration_list()

    # If there are no storage configurations, return an empty list
    if not storage_configuration_list:
        return []

    # Map the storage configuration id to each project
    return list(map(
        lambda project_iter: (
            cast(
                ProjectToStorageMappingDictModel,
                cast(
                    object,
                    {
                        "id": str(project_iter.id),
                        "name": project_iter.name,
                        "storageConfigurationId": str(project_iter.self_managed_storage_configuration.id),
                        "prefix": get_project_storage_configuration_prefix(
                            project_iter
                        ),
                    }
                )
            )
        ),
        list(filter(
            lambda project_iter: (
                    project_iter.self_managed_storage_configuration is not None
            ),
            list_projects()
        ))
    ))


def _set_project_to_storage_configuration_mapping():
    """
    Set the project to storage configuration mapping list,
    this will first attempt to get the list from an environment variable,
    if that is not set it will get the list from the API
    """
    global PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST
    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is None:
        PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST = _get_project_to_storage_configuration_env_mapping()

    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is not None:
        return

    # No environment variable set, so we need to get the list from the API
    # Which takes longer
    PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST = _get_project_to_storage_configuration_api_mapping()


def get_project_to_storage_configuration_mapping_list() -> List[ProjectToStorageMappingDictModel]:
    """
    Get the project to storage configuration mapping list, this will first check if the list has already been set,
    if not it will set the list using the _set_project_to_storage_configuration_mapping function
    The project to storage configuration mapping list is expected to be a list of projects with the following format:
    - id: str
    - name: str
    - storageConfigurationId: str
    - prefix: Optional[str]

    :return: The project to storage configuration mapping list
    :rtype: `List[ProjectToStorageMappingDictModel]`
    """
    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is None:
        _set_project_to_storage_configuration_mapping()
        return get_project_to_storage_configuration_mapping_list()
    return PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST


def get_project_id_by_s3_key_prefix(s3_key_prefix: str) -> Optional[str]:
    """
    Get the project id by the s3 key prefix

    :param s3_key_prefix: The s3 key prefix
    :return: The project id, or None if no project id can be found for this
    :rtype: Optional[str]
    """

    # Iterate through the storage configuration mapping dict
    for project_model in get_project_to_storage_configuration_mapping_list():
        project_model_prefix = (
            project_model.get('prefix', None)
            if project_model.get('prefix', None) is not None
            else ""
        )

        # Configuration model
        try:
            configuration_model = next(
                filter(
                    lambda config_iter: (
                            config_iter['id'] == project_model['storageConfigurationId']
                    ),
                    get_storage_configuration_list()
                )
            )
        except StopIteration:
            continue

        # Join the storage configuration prefix with the project prefix
        project_s3_key_prefix = str(urlunparse((
            S3_URI_SCHEME,
            configuration_model['bucketName'],
            str(Path(configuration_model['keyPrefix']) / project_model_prefix) + "/",
            None, None, None
        )))

        if s3_key_prefix.startswith(project_s3_key_prefix):
            return project_model['id']

    logger.warning(f"Could not find project id for s3 key prefix: {s3_key_prefix}")
    return None


def get_project_self_storage_configuration_subfolder(
        project_id: Union[UUID4, str]
) -> Optional[str]:
    """
    Get the prefix for a selfManagedStorageConfiguration
    """
    # FIXME - use libica on next libica release
    # Local import
    import requests

    headers = {
        "Accept": "application/vnd.illumina.v3+json",
        "Authorization": f"Bearer {get_icav2_configuration().access_token}",
    }

    # Get the response from the API
    response = requests.get(
        f"https://ica.illumina.com/ica/rest/api/projects/{project_id}/selfManagedStorageConfiguration",
        headers=headers,
    )

    # Check if the response is a 404
    if response.status_code == 404:
        return None

    # Raise an exception if the request was unsuccessful
    response.raise_for_status()

    return response.json()['storageConfigurationSubFolder']


# And vice-versa
def get_s3_key_prefix_by_project_id(
        project_id: Union[UUID4, str]
) -> Optional[str]:
    """
    Get the s3 key prefix for a project id, this is the root s3 key prefix that is used for this project,
    including any project specific subfolder if they have a self managed storage configuration

    :param project_id: The project id
    :return: The s3 key prefix for this project, or None if no s3 key prefix can be found for this project id
    :rtype: Optional[str]
    """
    # Local imports
    # Return Key Prefix with project name extension
    try:
        project_model = next(filter(
            lambda project_iter: (
                    project_iter['id'] == str(project_id)
            ),
            get_project_to_storage_configuration_mapping_list()
        ))
    except StopIteration:
        return get_project_self_storage_configuration_subfolder(project_id)

    project_model_prefix = (
        project_model.get('prefix', None)
        if project_model.get('prefix', None) is not None
        else ""
    )

    # Configuration model
    configuration_model = next(
        filter(
            lambda config_iter: (
                    config_iter['id'] == project_model['storageConfigurationId']
            ),
            get_storage_configuration_list()
        )
    )

    # Join the storage configuration prefix with the project prefix
    return str(urlunparse((
        S3_URI_SCHEME,
        configuration_model['bucketName'],
        str(
            Path(configuration_model['keyPrefix']) /
            project_model_prefix
        ) + "/",
        None, None, None
    )))


def convert_s3_uri_to_icav2_uri(s3_uri: str) -> str:
    """
    Convert S3 URI to ICAv2 URI

    :param s3_uri: The S3 URI to convert, expected to be in the format s3://{bucket_name}/{key_prefix}/{path/to/data_obj}
    :return: The corresponding ICAv2 URI, in the format icav2://{project_id}/{path/to/data_obj}
    :rtype: str
    """
    # Convert S3 URI to ICAv2 URI
    # Putting it all together
    # Determine which project id we are working with
    project_id = get_project_id_by_s3_key_prefix(s3_uri)

    if project_id is None:
        raise ValueError(f"Could not find project id for s3 uri: {s3_uri}, and cannot convert to icav2 uri without a project id")

    # Then remap to determine the root prefix of this project
    project_s3_prefix = get_s3_key_prefix_by_project_id(project_id)

    # Check if the project s3 prefix is a prefix of the s3 uri, if not we cannot convert to icav2 uri
    if project_s3_prefix is None:
        raise ValueError(f"Could not find project s3 prefix for project id: {project_id}, cannot convert s3 uri to icav2 uri without a project s3 prefix")

    return str(
        urlunparse(
            (
                ICAV2_URI_SCHEME,
                get_project_id_by_s3_key_prefix(s3_uri),
                # This path is then the relative path to the project s3 prefix
                str(Path(urlparse(s3_uri).path).relative_to(Path(urlparse(project_s3_prefix).path))) + (
                    "/" if s3_uri.endswith("/") else ""),
                None, None, None
            )
        )
    )


def convert_icav2_uri_to_s3_uri(icav2_uri: str) -> str:
    """
    Convert ICAv2 URI to S3 URI

    :param icav2_uri: The ICAv2 URI to convert, expected to be in the format icav2://{project_id}/{path/to/data_obj}
    :return: The corresponding S3 URI, in the format s3://{bucket_name}/{key_prefix}/{path/to/data_obj}
    :rtype: str
    """
    # Convert ICAv2 URI to S3 URI
    # Putting it all together, in-fact this is a little more straight forward
    # Than the inverse
    # Determine which project id we are working with
    from ...project import coerce_project_id_or_name_to_project_id

    # Get the project id from the icav2 uri netloc
    project_id = coerce_project_id_or_name_to_project_id(urlparse(icav2_uri).netloc)

    # Get the s3 key prefix for this project
    project_s3_prefix = get_s3_key_prefix_by_project_id(project_id)

    # Check if the project s3 prefix is None, if so we cannot convert to s3 uri
    if project_s3_prefix is None:
        raise ValueError(f"Could not find project s3 prefix for project id: {project_id}, cannot convert icav2 uri to s3 uri without a project s3 prefix")

    return str(
        urlunparse(
            (
                urlparse(project_s3_prefix).scheme,
                urlparse(project_s3_prefix).netloc,
                str(
                    Path(urlparse(project_s3_prefix).path).joinpath(
                        # Cannot join two abs paths so we need to strip the leading /
                        # We can then join the relative path to the project s3 prefix
                        Path(str(Path(urlparse(icav2_uri).path)).lstrip("/"))
                    )
                ) + ("/" if icav2_uri.endswith("/") else ""),
                None, None, None
            )
        )
    )


def convert_project_data_obj_to_s3_uri(project_data_obj: ProjectData) -> str:
    """
    Convert a ProjectData object to an S3 URI, this will use the project id and path of the ProjectData object to determine the corresponding S3 URI

    :param project_data_obj: The ProjectData object to convert to an S3 URI
    :return: The corresponding S3 URI, in the format s3://{bucket_name}/{key_prefix}/{path/to/data_obj}
    :rtype: str
    """
    # Convert ProjectData object to S3 URI
    project_s3_prefix = get_s3_key_prefix_by_project_id(str(project_data_obj.data.details.owning_project_id))

    return str(
        urlunparse(
            (
                urlparse(project_s3_prefix).scheme,
                urlparse(project_s3_prefix).netloc,
                (
                    # Join the project s3 prefix with the data path, ensuring to strip the leading / from the data path
                    # as we are joining to an absolute path
                    str(
                        Path(urlparse(project_s3_prefix).path) / Path(project_data_obj.data.details.path.lstrip("/"))
                    ) +
                    # Add a trailing slash if this is a folder, as s3 uris for folders should end with a slash
                    (
                        "/" if project_data_obj.data.details.data_type == FOLDER_DATA_TYPE else ""
                    )
                ),
                None, None, None
            )
        )
    )


# And vice-versa
def convert_s3_uri_to_project_data_obj(
        s3_uri: str,
        create_data_if_not_found: bool = False
) -> ProjectData:
    """
    Convert an S3 URI to a ProjectData object, this will use the project id and path of the S3 URI to determine the corresponding ProjectData object

    :param s3_uri: The S3 URI to convert, expected to be in the format s3://{bucket_name}/{key_prefix}/{path/to/data_obj}
    :param create_data_if_not_found: If true, create the data if it cannot be found, this is only applicable to paths or uris, if the data cannot be found and this parameter is false, an error will be raised

    :return: The corresponding ProjectData object
    :rtype: `ProjectData <https://umccr.github.io/libica/openapi/v3/docs/ProjectData/>`_
    """
    # Convert S3 URI to ProjectData object
    # Easiest to convert to icav2 uri first and then to project data object
    from ...project_data import convert_icav2_uri_to_project_data_obj
    return convert_icav2_uri_to_project_data_obj(
        convert_s3_uri_to_icav2_uri(s3_uri),
        create_data_if_not_found=create_data_if_not_found
    )


def unpack_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Unpack an S3 URI into the project id and path, this will use the project id and path of the S3 URI to determine the corresponding project id and path

    :param uri: The S3 URI to unpack, expected to be in the format s3://{bucket_name}/{key_prefix}/{path/to/data_obj}
    :return: A tuple of the project id and path, where the project id is the id of the project that this S3 URI belongs to, and the path is the path to the data object within the project
    :rtype: Tuple[str, str]
    """
    # Unpack S3 URI
    # Get the project id from the s3 uri
    from ...project_data import unpack_uri
    return unpack_uri(convert_s3_uri_to_icav2_uri(uri))
