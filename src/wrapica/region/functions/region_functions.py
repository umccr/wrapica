#!/usr/bin/env python3

# Standard imports
from typing import List, Optional, Union
from pydantic import UUID4

# Libica Api imports
from libica.openapi.v3 import ApiClient, ApiException
from libica.openapi.v3.api.region_api import RegionApi

# Libica model imports
from libica.openapi.v3.models import Region

# Local imports
from ...utils.logger import get_logger
from ...utils.configuration import get_icav2_configuration
from ...utils.miscell import is_uuid_format

# Get logger
logger = get_logger()

DEFAULT_REGION: Optional[Region] = None


def get_regions() -> List[Region]:
    """
    Return a list of regions

    :return: The list of regions available to the user in this tenant
    :rtype: List[`Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_]
    :raises ApiException: If an error occurs while retrieving the regions

    :Example:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_regions

        regions = get_regions()

        if len(regions) == 0:
            print("No regions found")
        else:
            print(f"Found {len(regions)} region(s)")
            for region in regions:
                print(f"Region ID: {region.id}, City Name: {region.city_name}")
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = RegionApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Retrieve a list of regions. Only the regions the user has access to through his/her entitlements are returned.
        api_response = api_instance.get_regions()
    except ApiException as e:
        logger.error("Exception when calling RegionApi->get_regions: %s\n" % e)
        raise ApiException

    return api_response.items


def get_region_obj_from_region_id(
        region_id: Union[UUID4, str]
) -> Region:
    """
    Get region object from the region id

    :param region_id:  The region ID
    :return: The region object
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_region_id

        region_id = "region-1234"
        region = get_region_obj_from_region_id(region_id)

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = RegionApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Get region
        api_response = api_instance.get_region(str(region_id))
    except ApiException as e:
        logger.error("Exception when calling RegionApi->get_regions: %s\n" % e)
        raise ApiException

    return api_response


def get_region_obj_from_city_name(city_name: str) -> Region:
    """
    Get the region id from the city name

    :param city_name: The city name
    :return: The region object

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_city_name

        city_name = "Sydney"
        region = get_region_obj_from_city_name(city_name)

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
    """
    try:
        return next(
            filter(
                lambda region_iter: region_iter.city_name == city_name,
                get_regions()
            )
        )
    except StopIteration:
        logger.error(f"Could not get region object from city name {city_name}")
        raise StopIteration


def coerce_region_id_or_city_name_to_region_obj(
        region_id_or_city_name: Union[UUID4, str]
) -> Region:
    """
    Given either a region id or a region city name, coerce to region object

    :param region_id_or_city_name: The region id or city name
    :return: The region object
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_
    """
    if is_uuid_format(region_id_or_city_name):
        return get_region_obj_from_region_id(region_id_or_city_name)
    return get_region_obj_from_city_name(region_id_or_city_name)


def coerce_region_id_or_city_name_to_region_id(
        region_id_or_city_name: Union[UUID4, str]
) -> str:
    """
    Given either a region id or a region city name, coerce to region id

    :param region_id_or_city_name:

    :return: The region id
    :rtype: str
    """
    if is_uuid_format(region_id_or_city_name):
        return str(region_id_or_city_name)
    return str(get_region_obj_from_city_name(region_id_or_city_name).id)


def get_region_obj_from_project_id(
        project_id: Union[UUID4, str]
) -> Region:
    """
    Collect the region object from the project id

    :param project_id: The project ID
    :return: The region object
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_project_id

        project_id = "project-1234"
        region = get_region_obj_from_project_id(project_id)
    """
    from ...project import get_project_obj_from_project_id
    return get_project_obj_from_project_id(project_id).region


def get_region_from_bundle_id(
        bundle_id: Union[UUID4, str]
) -> Region:
    """
    Get the region object from the bundle id

    :param bundle_id: The bundle ID

    :return: The region object
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_from_bundle_id

        bundle_id = "bundle-1234"
        region = get_region_from_bundle_id(bundle_id)
    """
    from ...bundle import get_bundle_obj_from_bundle_id
    bundle_obj = get_bundle_obj_from_bundle_id(bundle_id)
    return bundle_obj.region


def set_default_region():
    """
    Set the default region
    Assumes only one region is available to the user
    This is used by get_default_region,
    region is stored as a global python variable and is available for the rest of the session.

    """

    global DEFAULT_REGION

    regions = get_regions()

    if len(regions) == 0:
        raise Exception("No regions found, could not set default region")

    if not len(regions) == 1:
        raise Exception("Multiple regions found, cannot set default region")

    DEFAULT_REGION = regions[0]


def get_default_region() -> Region:
    """
    Get the default region, if no region is set, invocate set_default_region and assign global variable
    Used by bundle and project functions to get the default region when no region is provided

    :return: The default region object
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`

    """
    if DEFAULT_REGION is None:
        set_default_region()
    return DEFAULT_REGION

