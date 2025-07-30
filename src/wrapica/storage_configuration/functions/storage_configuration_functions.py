#!/usr/bin/env python3

# Standard imports
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlunparse, urlparse

# Libica imports
from libica.openapi.v3 import ApiException, ApiClient
from libica.openapi.v3.api.storage_configuration_api import StorageConfigurationApi
from libica.openapi.v3.models import (
    StorageConfigurationWithDetails,
    ProjectData,
    AWSDetails
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



# { "s3-key-prefix": {"storage_configuration_id": [project_id_1, project_id_2, ...]}
STORAGE_CONFIGURATION_MAPPING_DICT: Optional[Dict[str, Dict[str, List[str]]]] = None

def get_storage_configuration_list() -> List[StorageConfigurationWithDetails]:
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
            raise ApiException

    return api_response.items


def set_storage_configuration_mapping():
    global STORAGE_CONFIGURATION_MAPPING_DICT
    from wrapica.project import list_projects

    def get_s3_uri_from_aws_details(aws_details: AWSDetails) -> str:
        return str(
            urlunparse(
                (
                    S3_URI_SCHEME,
                    aws_details.bucket_name,
                    str(Path(aws_details.key_prefix)) + "/",
                    None, None, None
                )
            )
        )

    # Get the storage configuration list
    storage_configuration_list: List[StorageConfigurationWithDetails] = get_storage_configuration_list()

    # Map the storage configuration id to each project
    storage_configuration_mapping_by_project = dict(
        map(
            lambda byob_project_iter: (byob_project_iter.id, byob_project_iter.self_managed_storage_configuration.id),
            filter(
                lambda project_iter: (
                    hasattr(project_iter, 'self_managed_storage_configuration') and
                    project_iter.self_managed_storage_configuration is not None
                ),
                list_projects()
            )
        )
    )

    # Flip the storage configuration mapping from by project to by storage configuration
    storage_configuration_mapping = {}
    for storage_configuration in storage_configuration_list:
        storage_configuration_mapping[storage_configuration.id] = list(
            map(
                lambda kv: kv[0],
                filter(
                    lambda kv: kv[1] == storage_configuration.id,
                    storage_configuration_mapping_by_project.items()
                )
            )
        )

    # For each storage configuration, get the s3 key prefix
    STORAGE_CONFIGURATION_MAPPING_DICT = dict(
        map(
            lambda kv: (
                get_s3_uri_from_aws_details(
                    next(
                        filter(
                            lambda storage_config_iter: storage_config_iter.id == kv[0],
                            storage_configuration_list
                        )
                    ).storage_configuration_details.aws_s3
                ),
                {kv[0]: kv[1]}
            ),
            storage_configuration_mapping.items()
        )
    )


def get_storage_configuration_mapping() -> Dict[str, Dict[str, List[str]]]:
    # Check if the mapping is already set
    if STORAGE_CONFIGURATION_MAPPING_DICT is not None:
        return STORAGE_CONFIGURATION_MAPPING_DICT

    # Set the mapping
    set_storage_configuration_mapping()

    # Return the mapping
    return get_storage_configuration_mapping()


def get_project_id_by_s3_key_prefix(s3_key_prefix: str) -> Optional[str]:
    from ...project import get_project_id_from_project_name

    # Iterate through the storage configuration mapping dict
    for configuration_s3_key_prefix, project_configuration_dict in get_storage_configuration_mapping().items():
        # Check if the s3 key prefix is in the storage configuration dict
        if s3_key_prefix.startswith(configuration_s3_key_prefix):
            # Get the project name as the first part after the configuration s3 prefix
            project_name = Path(urlparse(s3_key_prefix).path).relative_to(Path(urlparse(configuration_s3_key_prefix).path)).parts[0]

            try:
                project_id = get_project_id_from_project_name(project_name)
            except (StopIteration, ApiException):
                logger.error(f"Could not find project id for project name: {project_name}")
                return None

            # Check project id is in this project configuration dict
            for storage_configuration_id, project_ids_list in project_configuration_dict.items():
                if project_id in project_ids_list:
                    return project_id
            logger.error(f"Got project name as '{project_name}' but could not find project id '{project_id}' in project configuration dict")

    logger.error(f"Could not find project id for s3 key prefix: {s3_key_prefix}")
    return None


# And vice-versa
def get_s3_key_prefix_by_project_id(project_id: str) -> Optional[str]:
    # Local imports
    from ...project import get_project_name_from_project_id
    # Return Key Prefix with project name extension
    for configuration_s3_key_prefix, project_configuration_dict in get_storage_configuration_mapping().items():
        for configuration_id, project_list in project_configuration_dict.items():
            if project_id in project_list:
                return str(urlunparse(
                    (
                        urlparse(configuration_s3_key_prefix).scheme,
                        urlparse(configuration_s3_key_prefix).netloc,
                        str(
                            Path(
                                urlparse(configuration_s3_key_prefix).path
                            ) /
                            get_project_name_from_project_id(project_id)
                        ) + "/",
                        None, None, None
                    )
                ))
    logger.warning("Could not get S3 key prefix for project id: %s", project_id)
    return None

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
