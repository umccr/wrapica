#!/usr/bin/env python3
# Standard imports
from os import environ
from pathlib import Path
from typing import List, Optional, Tuple, TypedDict, NotRequired, cast
from urllib.parse import urlunparse, urlparse
import json
from ruamel.yaml import YAML

# Libica imports
from libica.openapi.v3 import ApiException, ApiClient
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


def get_storage_configuration_env_list() -> Optional[List[StorageConfigurationObjectModel]]:
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


def get_storage_configuration_api_list() -> List[StorageConfigurationObjectModel]:
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
                {
                    "id": item_iter_.id,
                    "bucketName": f"s3://{item_iter_.storage_configuration_details.aws_s3.bucket_name}",
                    "keyPrefix": str(Path(item_iter_.storage_configuration_details.aws_s3.key_prefix)) + "/"
                }
            )
        ),
        api_response.items
    ))


def set_storage_configuration_list():
    global STORAGE_CONFIGURATION_OBJECT_LIST
    if STORAGE_CONFIGURATION_OBJECT_LIST is None:
        STORAGE_CONFIGURATION_OBJECT_LIST = get_storage_configuration_env_list()

    if STORAGE_CONFIGURATION_OBJECT_LIST is not None:
        return

    # No environment variable set, so we need to get the list from the API
    # Which takes longer
    STORAGE_CONFIGURATION_OBJECT_LIST = get_storage_configuration_api_list()


def get_storage_configuration_list() -> List[StorageConfigurationObjectModel]:
    if STORAGE_CONFIGURATION_OBJECT_LIST is None:
        set_storage_configuration_list()
        return get_storage_configuration_list()
    return STORAGE_CONFIGURATION_OBJECT_LIST


def get_project_to_storage_env_mapping() -> Optional[List[ProjectToStorageMappingDictModel]]:
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


def get_project_to_storage_configuration_api_mapping() -> List[ProjectToStorageMappingDictModel]:
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
              {
                    "id": project_iter.id,
                    "name": project_iter.name,
                    "storageConfigurationId": project_iter.self_managed_storage_configuration.id,
                    "prefix": project_iter.name,
              }
          )
      ),
      list(filter(
          lambda project_iter: (
              project_iter.self_managed_storage_configuration is not None
          ),
          list_projects()
      ))
    ))


def set_project_to_storage_configuration_mapping():
    global PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST
    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is None:
        PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST = get_project_to_storage_env_mapping()

    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is not None:
        return

    # No environment variable set, so we need to get the list from the API
    # Which takes longer
    PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST = get_project_to_storage_configuration_api_mapping()


def get_project_to_storage_configuration_mapping_list() -> List[ProjectToStorageMappingDictModel]:
    if PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST is None:
        set_project_to_storage_configuration_mapping()
        return get_project_to_storage_configuration_mapping_list()
    return PROJECT_TO_STORAGE_CONFIGURATION_MAPPING_LIST


def get_project_id_by_s3_key_prefix(s3_key_prefix: str) -> Optional[str]:
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

    logger.error(f"Could not find project id for s3 key prefix: {s3_key_prefix}")
    return None


# And vice-versa
def get_s3_key_prefix_by_project_id(project_id: str) -> Optional[str]:
    # Local imports
    # Return Key Prefix with project name extension
    project_model = next(filter(
        lambda project_iter: (
            project_iter['id'] == project_id
        ),
        get_project_to_storage_configuration_mapping_list()
    ))

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
            (project_model.get('prefix', None) if project_model.get('prefix', None) is not None else "")
        ) + "/",
        None, None, None
    )))


def convert_s3_uri_to_icav2_uri(s3_uri: str) -> str:
    # Convert S3 URI to ICAv2 URI
    # Putting it all together
    # Determine which project id we are working with
    project_id = get_project_id_by_s3_key_prefix(s3_uri)
    # Then remap to determine the root prefix of this project
    project_s3_prefix = get_s3_key_prefix_by_project_id(project_id)

    return str(
        urlunparse(
            (
                ICAV2_URI_SCHEME,
                get_project_id_by_s3_key_prefix(s3_uri),
                # This path is then the relative path to the project s3 prefix
                str(Path(urlparse(s3_uri).path).relative_to(Path(urlparse(project_s3_prefix).path))) + ("/" if s3_uri.endswith("/") else ""),
                None, None, None
            )
        )
    )


def convert_icav2_uri_to_s3_uri(icav2_uri: str) -> str:
    # Convert ICAv2 URI to S3 URI
    # Putting it all together, in-fact this is a little more straight forward
    # Than the inverse
    # Determine which project id we are working with
    from ...project import coerce_project_id_or_name_to_project_id

    # Get the project id from the icav2 uri netloc
    project_id = coerce_project_id_or_name_to_project_id(urlparse(icav2_uri).netloc)

    # Get the s3 key prefix for this project
    project_s3_prefix = get_s3_key_prefix_by_project_id(project_id)

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
    # Convert ProjectData object to S3 URI
    project_s3_prefix = get_s3_key_prefix_by_project_id(project_data_obj.data.details.owning_project_id)

    return str(
        urlunparse(
            (
                urlparse(project_s3_prefix).scheme,
                urlparse(project_s3_prefix).netloc,
                str(Path(urlparse(project_s3_prefix).path) / Path(project_data_obj.data.details.path.lstrip("/"))) + ("/" if project_data_obj.data.details.data_type == FOLDER_DATA_TYPE else ""),
                None, None, None
            )
        )
    )


# And vice-versa
def convert_s3_uri_to_project_data_obj(s3_uri: str, create_data_if_not_found: bool = False) -> ProjectData:
    # Convert S3 URI to ProjectData object
    # Easiest to convert to icav2 uri first and then to project data object
    from ...project_data import convert_icav2_uri_to_project_data_obj
    return convert_icav2_uri_to_project_data_obj(
        convert_s3_uri_to_icav2_uri(s3_uri),
        create_data_if_not_found=create_data_if_not_found
    )


def unpack_s3_uri(uri: str) -> Tuple[str, str]:
    # Unpack S3 URI
    # Get the project id from the s3 uri
    from ...project_data import unpack_uri
    return unpack_uri(convert_s3_uri_to_icav2_uri(uri))
