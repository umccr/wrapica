#!/usr/bin/env python3

"""
Storage Credentials Functions

This module provides functions mainly to resolve URIs to storage credentials.

This is particularly useful for running analyses using the 'externalData' option.
"""
# Imports
from pathlib import Path
from typing import TypedDict, NotRequired, List, Optional, cast, Union
from os import environ
from urllib.parse import urlunparse, urlparse
from pydantic import UUID4

# Libica imports
from libica.openapi.v3 import ApiException, ApiClient, StorageCredentialList
from libica.openapi.v3.api.storage_credentials_api import StorageCredentialsApi
from ruamel.yaml import YAML

# Local imports
from ...utils.configuration import get_icav2_configuration
from ...utils.logger import get_logger
from ...utils.globals import (
    S3_URI_SCHEME,
)

# Get the logger
logger = get_logger()


# Class
class S3UriListModel(TypedDict):
    bucketName: str
    keyPrefix: NotRequired[str]


class StorageCredentialMappingModel(TypedDict):
    id: str
    name: str
    s3UriList: List[S3UriListModel]


STORAGE_CREDENTIAL_OBJECT_LIST: Optional[List[StorageCredentialMappingModel]] = None
STORAGE_CREDENTIAL_OBJECT_LIST_ENV_VAR = "ICAV2_STORAGE_CREDENTIAL_LIST_FILE"


def get_storage_credential_env_list() -> Optional[List[StorageCredentialMappingModel]]:
    if (
            environ.get(STORAGE_CREDENTIAL_OBJECT_LIST_ENV_VAR) is not None and
            Path(environ.get(STORAGE_CREDENTIAL_OBJECT_LIST_ENV_VAR)).is_file()
    ):
        yaml = YAML()

        with open(environ.get(STORAGE_CREDENTIAL_OBJECT_LIST_ENV_VAR)) as file_handle:
            # Load the YAML file
            return list(map(
                lambda item_iter_: cast(
                    StorageCredentialMappingModel,
                    item_iter_
                ),
                yaml.load(file_handle)
            ))
    return None



def get_storage_credential_api_list() -> List[StorageCredentialMappingModel]:
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = StorageCredentialsApi(api_client)

    try:
        # Retrieve a list of the storage credentials
        api_response: StorageCredentialList = api_instance.get_storage_credentials()
    except ApiException as e:
        logger.error("Exception when calling StorageCredentialsApi->get_storage_credentials: %s\n" % e)
        raise ApiException from e

    return list(map(
        lambda item_iter_: (
            cast(
                StorageCredentialMappingModel,
                cast(
                    object,
                    {
                        "id": item_iter_.id,
                        "name": item_iter_.name,
                        # Unfortunately there is no way to link storage configurations to storage credentials at the moment
                        # I have raised the issue with the Illumina team
                        "s3UriList": []
                    }
                )
            )
        ),
        api_response.items
    ))


def set_storage_credential_list():
    global STORAGE_CREDENTIAL_OBJECT_LIST
    if STORAGE_CREDENTIAL_OBJECT_LIST is None:
        STORAGE_CREDENTIAL_OBJECT_LIST = get_storage_credential_env_list()

    if STORAGE_CREDENTIAL_OBJECT_LIST is not None:
        return

    # No environment variable set, so we need to get the list from the API
    # Which takes longer
    STORAGE_CREDENTIAL_OBJECT_LIST = get_storage_credential_api_list()


def get_storage_credential_list() -> List[StorageCredentialMappingModel]:
    if STORAGE_CREDENTIAL_OBJECT_LIST is None:
        set_storage_credential_list()
        return get_storage_credential_list()
    return STORAGE_CREDENTIAL_OBJECT_LIST


def get_storage_credential_id_from_s3_uri(s3_uri: str) -> Optional[str]:
    """
    Get the storage credential id from an s3 uri
    :param s3_uri:
    :return:
    """
    for credential_obj in get_storage_credential_list():
        for s3_uri_list in credential_obj['s3UriList']:
            if (
                    s3_uri.startswith(
                        str(urlunparse((
                                S3_URI_SCHEME,
                                s3_uri_list['bucketName'],
                                s3_uri_list['keyPrefix'],
                                None, None, None
                        )))
                    )
            ):
                return credential_obj['id']

    return None


def get_relative_path_from_credentials_prefix(
        storage_credential_id: Union[UUID4, str],
        s3_uri: str
) -> str:
    """
    Get the appropriate mount path for an analysis given a
    storage credential id and an s3 uri
    :param storage_credential_id:
    :param s3_uri:
    :return:
    """
    # Get credential object
    credential_object = next(filter(
        lambda credential_iter_: credential_iter_['id'] == str(storage_credential_id),
        get_storage_credential_list()
    ))

    s3_uri_object: S3UriListModel = next(filter(
        lambda item_iter_: (
            item_iter_['bucketName'] == urlparse(s3_uri).netloc
            and
            urlparse(s3_uri).path.lstrip("/").startswith(item_iter_['keyPrefix'])
        ),
        credential_object['s3UriList']
    ))

    # Return s3 uri
    s3_rel_path = Path(
        urlparse(
            s3_uri
        ).path.lstrip("/")
    ).relative_to(
        Path(
            s3_uri_object['keyPrefix']
        )
    )

    # re add the '/' if omitted
    return str(s3_rel_path) + ("/" if s3_uri.endswith("/") else "")
