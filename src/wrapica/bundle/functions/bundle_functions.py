#!/usr/bin/env python

# Standard imports
from pathlib import Path
from typing import List, Optional, Union
import re

# Libica api imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.bundle_api import BundleApi
from libica.openapi.v3.api.bundle_data_api import BundleDataApi
from libica.openapi.v3.api.bundle_pipeline_api import BundlePipelineApi
from libica.openapi.v3.api.project_api import ProjectApi

# Libica model imports
from libica.openapi.v3.models import (
    Data,
    BundleData,
    BundlePagedList,
    BundlePipeline,
    BundlePipelineList,
    CreateBundle,
    Bundle,
    Links
)
from pydantic import UUID4

# Local imports
from ...literals import BundleStatusType
from ...pipelines.functions.pipelines_functions import get_pipeline_obj_from_pipeline_id
from ...utils.configuration import get_icav2_configuration
from ...utils.globals import (
    LIBICAV2_DEFAULT_PAGE_SIZE,
    FILE_DATA_TYPE,
    FOLDER_DATA_TYPE
)

# Set logger
from ...utils.logger import get_logger
from ...utils.miscell import is_uuid_format, coerce_to_uuid4_obj

logger = get_logger()


def generate_empty_bundle(
        bundle_name: str,
        bundle_version: str,
        short_description: str,
        version_comment: str,
        region_id: Optional[Union[UUID4, str]] = None,
        categories: Optional[List[str]] = None,
        pipeline_release_url: Optional[str] = None
) -> Bundle:
    """
    Generate an empty bundle (in DRAFT format) and return the bundle object

    :param bundle_name:  The name of the bundle
    :param bundle_version:  The version of the bundle
    :param short_description:  The description of the bundle
    :param version_comment:  The description of the version
    :param region_id:  The region id of the bundle
    :param categories:
    :param pipeline_release_url:

    :return: The newly created bundle object
    :rtype: `Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python

        :linenos:

        from wrapica.bundle import generate_empty_bundle
        from wrapica.libica_models import Bundle

        # Initialise bundle
        bundle_obj: Bundle = generate_empty_bundle(
            bundle_name="my-first-bundle",
            bundle_description="A quick description of the bundle",
            bundle_version="1.0.0",
            bundle_version_description="First release of my-first-bundle",
            categories=["Test"]
        )

    """
    from ...region import get_default_region

    # Pipeline release url will soon be used when #156 is resolved
    _ = pipeline_release_url

    # Check if region id is set
    if region_id is None:
        region_id = get_default_region().id

    # Check if categories is set
    if categories is None:
        categories = []

    # Configuration needs manual work with custom access token
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = BundleApi(api_client)

    create_bundle = CreateBundle(
        name=bundle_name,
        shortDescription=short_description,
        bundleReleaseVersion=bundle_version,
        bundleVersionComment=version_comment,
        regionId=coerce_to_uuid4_obj(region_id),
        bundleStatus="DRAFT",
        categories=categories,
        links=Links(
            links=[
                # Drop link while https://github.com/umccr-illumina/ica_v2/issues/156 is still active
                # Link(
                #     name="GitHub CWL-ICA Release Page",
                #     url=pipeline_release_url
                # )
            ],
            licenses=[],
            homepages=[],
            publications=[]
        ),
        metadataModelId=None
    )

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Create a new bundle
        api_response: Bundle = api_instance.create_bundle(create_bundle=create_bundle)
    except ApiException as e:
        logger.error("Exception when calling BundleApi->create_bundle: %s\n" % e)
        raise ApiException

    return api_response


def get_bundle_obj_from_bundle_id(
        bundle_id: Union[UUID4, str]
) -> Bundle:
    """
    Given a bundle_id, return the bundle object

    :param bundle_id:

    :return: The bundle as an object
    :rtype: `Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.bundle import get_bundle_obj_from_bundle_id
        from wrapica.libica_models import Bundle

        # Get bundle object from the bundle id
        bundle_obj: Bundle = get_bundle_obj_from_bundle_id(
            bundle_id='abcdef-1234'
        )

    """

    # Initialise the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = BundleApi(api_client)

    try:
        # Get a bundle by ID.
        api_response: Bundle = api_instance.get_bundle(
            bundle_id=str(bundle_id)
        )
    except ApiException as e:
        logger.error("Exception when calling BundleApi->get_bundle_by_id: %s\n" % e)
        raise ApiException

    return api_response


def get_bundle_obj_from_bundle_name(
        bundle_name: str,
        region_id: Optional[Union[UUID4, str]] = None
) -> Bundle:
    """
    Given a bundle name, return the bundle object

    :param bundle_name:  The bundle name
    :param region_id:  The region id of the bundle

    :return: The bundle as an object

    :rtype: `Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.bundle import get_bundle_obj_from_bundle_name
        from wrapica.libica_models import Bundle

        # Get bundle object from the bundle name
        bundle_obj: Bundle = get_bundle_obj_from_bundle_name(
            bundle_name='my-first-bundle'
        )

    """
    bundle_list = filter_bundles(
        bundle_name=bundle_name,
        region_id=region_id
    )

    if len(bundle_list) == 0:
        logger.error(f"Could not get bundle object from {bundle_name}")
        raise IndexError
    if len(bundle_list) > 1:
        logger.error(f"Multiple bundles found with name {bundle_name}")
        raise IndexError

    return bundle_list[0]


def add_pipeline_to_bundle(
        bundle_id: Union[UUID4, str],
        pipeline_id: Union[UUID4, str]
) -> bool:
    """
    Add a pipeline to a bundle.

    :param bundle_id:
    :param pipeline_id:

    :return: True if successful, False otherwise
    :rtype: bool

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Wrapica imports
        from wrapica.pipeline import get_pipeline_obj_from_pipeline_id
        from wrapica.bundle import add_pipeline_to_bundle

        # Set vars
        pipeline_code = "my_pipeline_code"
        bundle_name = "my_bundle_name"

        # Get pipeline id from pipeline code
        pipeline_id = get_pipeline_obj_from_pipeline_id(pipeline_code).id

        # Get bundle id from bundle name
        bundle_id = get_bundle_obj_from_bundle_name(bundle_name).id

        # Link pipeline to bundle
        add_pipeline_to_bundle(bundle_id, pipeline_id)
    """

    # Check Pipeline Status
    pipeline_obj = get_pipeline_obj_from_pipeline_id(pipeline_id)
    if not pipeline_obj.status == "RELEASED":
        logger.warning(
            f"Pipeline '{pipeline_id}' is not released. Please release the pipeline before adding it to a bundle")
        return False

    # Get bundle object to confirm it exists
    try:
        _ = get_bundle_obj_from_bundle_id(bundle_id)
    except ApiException as e:
        logger.error(f"Could not get bundle '{bundle_id}', {e}")
        raise ApiException

    # Initialise the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Set accept header for adding pipeline to bundle
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        # Create an instance of the API class
        api_instance = BundlePipelineApi(api_client)

    try:
        # Link a pipeline to a bundle.
        api_instance.link_pipeline_to_bundle(
            bundle_id=str(bundle_id),
            pipeline_id=str(pipeline_id),
        )
        return True
    except ApiException as e:
        logger.error("Exception when calling BundlePipelineApi->link_pipeline_to_bundle: %s\n" % e)
        raise ApiException


def add_project_data_to_bundle(
    bundle_id: Union[UUID4, str],
    project_id: Union[UUID4, str],
    data_id: Union[UUID4, str]
) -> bool:
    """
    Add project data to a bundle

    :param bundle_id:
    :param project_id:
    :param data_id:

    :return: True if successful, False otherwise

    :rtype: bool

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import add_project_data_to_bundle
        from wrapica.project_data import convert_icav2_uri_to_data_obj

        # Set vars
        bundle_id = "abcdef-1234"
        data_uri = "icav2://project_id/path/to/data"

        # Get project id and data id from data uri
        project_data_obj = convert_icav2_uri_to_data_obj(data_uri)

        # Add project data to bundle
        add_project_data_to_bundle(bundle_id, project_data_obj.project_id, project_data_obj.data.id)
    """

    # Get Bundle
    bundle_obj = get_bundle_obj_from_bundle_id(bundle_id)

    # Get the data object
    from ...project_data import get_project_data_obj_by_id
    project_data_obj = get_project_data_obj_by_id(project_id, data_id)

    # Confirm data region and bundle region are the same
    if not str(project_data_obj.data.details.region.id) == str(bundle_obj.region.id):
        logger.error(f"Data region '{project_data_obj.data.details.region.code}' and Bundle region '{bundle_obj.region.code}' are not the same")
        return False

    # Initialise the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = BundleDataApi(api_client)

    try:
        # Link a data to a bundle.
        api_instance.link_data_to_bundle(
            bundle_id=str(bundle_id),
            data_id=str(project_data_obj.data.id)
        )
        return True
    except ApiException as e:
        logger.error("Exception when calling BundleDataApi->link_data_to_bundle: %s\n" % e)
        raise ApiException


def add_data_to_bundle(
        bundle_id: Union[UUID4, str],
        data_id: Union[UUID4, str]
) -> bool:
    """
    Add data to a bundle

    :param bundle_id:
    :param data_id:

    :return: True if successful, False otherwise
    :rtype: bool

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import add_project_data_to_bundle
        from wrapica.data import convert_icav2_uri_to_data_obj

        # Set vars
        # Use get_bundle_obj_from_bundle_name to get bundle_id if
        # you have the bundle name and not the bundle id
        bundle_id = "abcdef-1234"
        data_uri = "icav2://project_id/path/to/data"

        # Add project data to bundle
        add_data_to_bundle(bundle_id, convert_icav2_uri_to_data_obj(data_uri).id)
    """

    # Get Bundle
    bundle_obj = get_bundle_obj_from_bundle_id(bundle_id)

    # Get the data object
    from ...data import get_data_obj_from_data_id
    data_obj: Data = get_data_obj_from_data_id(data_id)

    # Confirm data region and bundle region are the same
    if not str(data_obj.details.region.id) == str(bundle_obj.region.id):
        logger.error(f"Data region '{data_obj.details.region.id}' and Bundle region '{bundle_obj.region.id}' are not the same")
        return False

    # Initialise the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        api_instance = BundleDataApi(api_client)

    try:
        # Link a data to a bundle.
        api_instance.link_data_to_bundle(
            bundle_id=str(bundle_id),
            data_id=str(data_id)
        )
        return True
    except ApiException as e:
        logger.error("Exception when calling BundleDataApi->link_data_to_bundle: %s\n" % e)
        raise ApiException


def release_bundle(
    bundle_id: Union[UUID4, str]
):
    """
    Release a bundle, converts a bundle status from DRAFT to RELEASED.

    :param bundle_id:

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import release_bundle

        # Set vars
        bundle_id = "abcdef-1234"

        # Release bundle
        release_bundle(bundle_id)
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        # Create an instance of the API class
        api_instance = BundleApi(api_client)

    try:
        # release a bundle
        api_instance.release_bundle(str(bundle_id))
    except ApiException as e:
        logger.error("Exception when calling BundleApi->release_bundle: %s\n" % e)
        raise ApiException

    logger.info(f"Successfully released '{bundle_id}'")


def filter_bundles(
    bundle_name: Optional[str] = None,
    project_id: Optional[Union[UUID4, str]] = None,
    region_id: Optional[Union[UUID4, str]] = None,
    status: Optional[BundleStatusType] = None,
    creator_id: Optional[Union[UUID4, str]] = None
) -> Optional[List[Bundle]]:
    """
    Get a list of bundles but filter by name, region id, status, and creator id

    :param bundle_name:  The name of the bundle
    :param project_id: The project id
    :param region_id:  The region id of the bundle
    :param status:  The status of the bundle
    :param creator_id:  The creator id of the bundle

    :return: List of bundles

    :rtype: List[`Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import filter_bundles
        from wrapica.libica_models import Bundle
        from wrapica.enums import BundleStatus
        from wrapica.user import get_user_from_user_name

        # Set vars
        user_name = "Alexis Lucattini"

        # Get creator id from user name
        creator_id = get_user_from_user_name(user_name).id

        # Filter bundles
        bundle_list: List[Bundle] = filter_bundles(
            bundle_status=BundleStatus.RELEASED,
            creator_id=creator_id
        )
    """

    if project_id is not None:
        # Use the project/bundle endpoint instead
        bundle_list = list_bundles_in_project(project_id)
    else:
        # Enter a context with an instance of the API client
        with ApiClient(get_icav2_configuration()) as api_client:
            # Create an instance of the API class
            api_instance = BundleApi(api_client)

        # example passing only required values which don't have defaults set
        bundle_list = []
        page_size = LIBICAV2_DEFAULT_PAGE_SIZE
        page_offset = 0

        try:
            while True:
                # Retrieve a bundle.
                api_response: BundlePagedList = api_instance.get_bundles(
                    page_size=str(page_size),
                    page_offset=str(page_offset)
                )
                bundle_list.extend(api_response.items)

                if page_offset + page_size >= api_response.total_item_count:
                    break
                page_offset += page_size

        except ApiException as e:
            logger.error("Exception when calling BundleApi->get_bundle: %s\n" % e)
            raise ApiException

        # Get each bundle by id
        # Must do this manually due to
        # bundle_obj_list = []
        # for bundle in bundle_list:
        #     try:
        #         bundle_obj_list.append(get_bundle_obj_from_bundle_id(bundle.get("id")))
        #     except TypeError:
        #         logger.warning(f"Could not convert into bundle obj from id {bundle.get('id')} ")

    bundle: Bundle
    returned_bundle_list: List[Bundle] = []
    if bundle_name is not None:
        bundle_name_regex = re.compile(bundle_name)
    else:
        bundle_name_regex = None

    for bundle in bundle_list:
        # Check if name matches
        if bundle_name_regex is not None and not bundle_name_regex.fullmatch(bundle.name):
            continue
        # Check region ids match
        if region_id is not None and not str(bundle.region.id) == str(region_id):
            continue
        # Check bundle status match
        if status is not None and not bundle.status == status:
            continue
        # Check creator id
        if creator_id is not None and not str(bundle.owner_id) == str(creator_id):
            continue

        returned_bundle_list.append(bundle)

    return returned_bundle_list


def list_data_in_bundle(
    bundle_id: Union[UUID4, str]
) -> List[BundleData]:
    """
    Given a bundle id, list data in a bundle

    :param bundle_id:

    :return: List of data items
    :rtype: List[`BundleData <https://umccr.github.io/libica/openapi/v3/docs/BundleData/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import list_data_in_bundles

        list_data_in_bundle(bundle_id='abcdef-1234')

    """
    data_items = []

    page_size = LIBICAV2_DEFAULT_PAGE_SIZE
    page_offset = 0

    # Start the loop
    while True:
        # Use the curl api for now
        # example passing only required values which don't have defaults set
        with ApiClient(get_icav2_configuration()) as api_client:
            api_instance = BundleDataApi(api_client)

        try:
            # Retrieve the list of bundle data.
            api_response = api_instance.get_bundle_data(
                bundle_id=str(bundle_id),
                page_size=str(page_size),
                page_offset=str(page_offset)
            )
        except ApiException as e:
            logger.error("Exception when calling BundleDataApi->get_bundle_data: %s\n" % e)
            raise ApiException

        data_items.extend(api_response.items)

        # Check page offset and page size against total item count
        if page_offset + page_size > api_response.total_item_count:
            break
        page_offset += page_size

    return data_items


def filter_bundle_data_to_top_level_only(
        bundle_data: List[BundleData]
) -> List[BundleData]:
    """
    Filter bundle data to top level only (no subdirectories or files underneath folders)

    :param bundle_data: List of linked bundle data items

    :return: List of top level bundle data items
    :rtype: List[`BundleData <https://umccr.github.io/libica/openapi/v3/docs/BundleData/>`_]

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import filter_bundle_data_to_top_level_only, list_data_in_bundle

        # Set vars
        bundle_id = "abcdef-1234"

        # Get all data
        bundle_data = list_data_in_bundle(bundle_id)

        # Filter to top level only
        top_level_bundle_data = filter_bundle_data_to_top_level_only(bundle_data)
    """
    # Find set of project ids
    project_ids: List[str] = list(set(list(map(
        lambda bundle_data_iter: str(bundle_data_iter.data.details.owning_project_id),
        bundle_data
    ))))

    # Collect bundle data by owning project id
    bundle_data_by_owning_project_id = {
        project_id: list(
            filter(
                lambda bundle_data_iter: str(bundle_data_iter.data.details.owning_project_id) == str(project_id),
                bundle_data
            )
        )
        for project_id in project_ids
    }

    # Find top level folders in bundle data by removing all subdirectories from list
    top_level_bundle_data_by_owning_project_id = {}
    for project_id, project_bundle_data_list in bundle_data_by_owning_project_id.items():
        # Initialise top level bundle data list for this project
        top_level_bundle_data_folder_list = []
        top_level_bundle_data_file_list = []

        all_folders_sorted = sorted(
            filter(
                lambda bundle_data_iter: bundle_data_iter.data.details.data_type == FOLDER_DATA_TYPE,
                project_bundle_data_list
            ),
            key=lambda bundle_data_sort_iter: bundle_data_sort_iter.data.details.path
        )

        # Find top folders only (where folder is not a child of another folder)
        for index_i, bundle_folder_data_iter_i in enumerate(all_folders_sorted):
            # Skip files
            if not bundle_folder_data_iter_i.data.details.data_type == FOLDER_DATA_TYPE:
                continue
            if any(
                map(
                    lambda bundle_folder_data_iter_j:
                    Path(bundle_folder_data_iter_i.data.details.path).is_relative_to(bundle_folder_data_iter_j.data.details.path),
                    all_folders_sorted[:index_i]
                )
            ):
                continue
            top_level_bundle_data_folder_list.append(bundle_folder_data_iter_i)

        # Find top level files in bundle data by only selecting files that are not children of folders
        for bundle_file_data_iter_i in project_bundle_data_list:
            # Skip folders
            if not bundle_file_data_iter_i.data.details.data_type == FILE_DATA_TYPE:
                continue
            if any(
                map(
                    lambda bundle_data_iter_j:
                        Path(bundle_file_data_iter_i.data.details.path).is_relative_to(Path(bundle_data_iter_j.data.details.path)),
                    top_level_bundle_data_folder_list
                )
            ):
                continue
            top_level_bundle_data_file_list.append(bundle_file_data_iter_i)

        # Add to top level bundle data by owning project id
        top_level_bundle_data_by_owning_project_id[project_id] = top_level_bundle_data_folder_list + top_level_bundle_data_file_list

    return list(
        data_iter
        for project_id in top_level_bundle_data_by_owning_project_id.values()
        for data_iter in project_id
    )


def list_pipelines_in_bundle(
    bundle_id: Union[UUID4, str]
) -> List[BundlePipeline]:
    """
    Given a bundle id, list pipelines in a bundle

    :param bundle_id:  The bundle id

    :return: List of pipeline items
    :rtype: List[`BundlePipeline <https://umccr.github.io/libica/openapi/v3/docs/BundlePipeline/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import list_pipelines_in_bundle

        list_pipelines_in_bundle(bundle_id='abcdef-1234')

    """
    # Use the curl api for now
    # No looping for pipelines
    # Enter a context with an instance of the API client
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = BundlePipelineApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a list of bundle pipelines.
        api_response: BundlePipelineList = api_instance.get_bundle_pipelines(str(bundle_id))
    except ApiException as e:
        logger.error("Exception when calling BundlePipelineApi->get_bundle_pipelines: %s\n" % e)
        raise ApiException

    return api_response.items


def list_bundles_in_project(
    project_id: Union[UUID4, str]
) -> List[Bundle]:
    """
    List bundles in a project

    :param project_id: The project id

    :return: List of bundles linked to this project
    :rtype: List[`Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_]

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import list_bundles_in_project

        list_bundles_in_project(project_id='abcdef-1234')    
    """
    # while True: no iterator for bundles list
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve project bundles.
        api_response = api_instance.get_project_bundles(str(project_id))
    except ApiException as e:
        logger.warning("Exception when calling ProjectApi->get_project_bundles: %s\n" % e)
        raise ApiException

    return list(map(lambda x: x.bundle, api_response.items))


def link_bundle_to_project(
    project_id: Union[UUID4, str],
    bundle_id: Union[UUID4, str]
):
    """
    Link bundle to project

    :param project_id:  The project id
    :param bundle_id:   The bundle id

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import link_bundle_to_project

        # Set vars
        project_id = "abcdef-1234"
        bundle_id = "abcdef-1234"

        # Link bundle to project
        link_bundle_to_project(project_id, bundle_id)
    """
    # Check bundle list
    existing_bundles: List[Bundle] = list_bundles_in_project(project_id)

    if any(map(
            lambda bundle_obj: str(bundle_obj.id) == str(bundle_id),
            existing_bundles
    )):
        logger.info(f"Bundle {bundle_id} already in project {project_id}")
        return

    # while True: no iterator for bundles list
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = ProjectApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Link bundle to project
        api_instance.link_project_bundle(
            project_id=str(project_id),
            bundle_id=coerce_to_uuid4_obj(bundle_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->link_project_bundle: %s\n" % e)
        raise ApiException

    logger.info(f"Successfully linked bundle {bundle_id} to project {project_id}")


def unlink_bundle_from_project(
        project_id: Union[UUID4, str],
        bundle_id: Union[UUID4, str],
):
    """
    Remove the bundle from the project

    :param project_id:  The project id
    :param bundle_id:   The bundle id

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import unlink_bundle_from_project

        # Set vars
        project_id = "abcdef-1234"
        bundle_id = "abcdef-1234"

        # Unlink bundle to project
        unlink_bundle_from_project(project_id, bundle_id)
    """
    # Check bundle list
    existing_bundles: List[Bundle] = list_bundles_in_project(project_id)

    # Map bundle id to bundle object
    if not any(map(
            lambda bundle_iter_: str(bundle_iter_.id) == str(bundle_id),
            existing_bundles
    )):
        logger.error(f"Bundle '{bundle_id}' is not in this project '{project_id}'")
        raise ValueError

    # while True: no iterator for bundles list
    with ApiClient(get_icav2_configuration()) as api_client:
        # DELETE endpoint needs accept header set
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        # Create an instance of the API class
        api_instance = ProjectApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Link bundle to project
        api_instance.unlink_project_bundle(
            project_id=project_id,
            bundle_id=coerce_to_uuid4_obj(bundle_id)
        )
    except ApiException as e:
        logger.error("Exception when calling ProjectApi->unlink_project_bundle: %s\n" % e)
        raise ApiException

    logger.info(f"Successfully unlinked bundle {bundle_id} from project {project_id}")


def deprecate_bundle(
        bundle_id: Union[UUID4, str]
):
    """
    Given a bundle id, deprecate the bundle

    :param bundle_id:
    :return:

    :raises: ApiException

    :Examples:

    .. code-block:: python
        :linenos:

        # Imports
        from wrapica.bundle import deprecate_bundle

        # Set vars
        bundle_id = "abcdef-1234"

        # Deprecate the bundle
        deprecate_bundle(bundle_id)
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        api_client.set_default_header(
            header_name="Accept",
            header_value="application/vnd.illumina.v3+json"
        )
        api_instance = BundleApi(api_client)

    try:
        # Deprecate a bundle
        api_instance.deprecate_bundle(str(bundle_id))
    except ApiException as e:
        logger.error("Exception when calling BundleApi->deprecate_bundle: %s\n" % e)
        raise ApiException

    logger.info(f"Successfully deprecated '{bundle_id}'")


def coerce_bundle_id_or_name_to_bundle_obj(
        bundle_id_or_name: str
) -> Bundle:
    """
    Given either a bundle id or bundle name, return the bundle object

    :param bundle_id_or_name:

    :return: The bundle as an object
    :rtype: `Bundle <https://umccr.github.io/libica/openapi/v3/docs/Bundle/>`_
    """
    if is_uuid_format(bundle_id_or_name):
        return get_bundle_obj_from_bundle_id(bundle_id_or_name)
    return get_bundle_obj_from_bundle_name(bundle_id_or_name)


def coerce_bundle_id_or_name_to_bundle_id(
        bundle_id_or_name: str
) -> str:
    """
    Given either a bundle id or bundle name, return the bundle id

    :param bundle_id_or_name:

    :return: The bundle id
    :rtype: str
    """
    if is_uuid_format(bundle_id_or_name):
        return bundle_id_or_name
    return str(get_bundle_obj_from_bundle_name(bundle_id_or_name).id)
