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
    Return a list of regions available to the user in this tenant.

    :return: The list of regions available to the user
    :rtype: List[`Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_]

    :raises ApiException: If the API call to retrieve regions fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_regions

        regions = get_regions()

        print(f"Found {len(regions)} region(s)")
        # Found 3 region(s)
        for region in regions:
            print(f"Region ID: {region.id}, City Name: {region.city_name}")
            # Region ID: abcd1234-..., City Name: Sydney
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
    Return the region object for a given region ID.

    :param region_id: The region identifier as a UUID4 object or UUID-formatted string

    :return: The region object matching the given ID
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :raises ApiException: If the API call to retrieve the region fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_region_id

        region = get_region_obj_from_region_id("abcd1234-ab12-ab12-ab12-abcdef123456")

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
        # Region ID: abcd1234-ab12-ab12-ab12-abcdef123456, City Name: Sydney
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = RegionApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Get region
        api_response = api_instance.get_region(region_id=str(region_id))
    except ApiException as e:
        logger.error("Exception when calling RegionApi->get_region: %s\n" % e)
        raise ApiException

    return api_response


def get_region_obj_from_city_name(city_name: str) -> Region:
    """
    Return the region object matching the given city name.

    :param city_name: The city name to look up in the available regions

    :return: The region object whose city name matches the input
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :raises StopIteration: If no region matches the given city name

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_city_name

        region = get_region_obj_from_city_name("Sydney")

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
        # Region ID: abcd1234-ab12-ab12-ab12-abcdef123456, City Name: Sydney
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
    Coerce a region ID or city name to a region object.

    :param region_id_or_city_name: The region identifier as a UUID4 or a city name string

    :return: The region object resolved from the input
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import coerce_region_id_or_city_name_to_region_obj

        region = coerce_region_id_or_city_name_to_region_obj("Sydney")

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
        # Region ID: abcd1234-ab12-ab12-ab12-abcdef123456, City Name: Sydney
    """
    if is_uuid_format(region_id_or_city_name):
        return get_region_obj_from_region_id(region_id_or_city_name)
    return get_region_obj_from_city_name(region_id_or_city_name)


def coerce_region_id_or_city_name_to_region_id(
        region_id_or_city_name: Union[UUID4, str]
) -> str:
    """
    Coerce a region ID or city name to a region ID string.

    :param region_id_or_city_name: The region identifier as a UUID4 or a city name string

    :return: The region ID as a string
    :rtype: str

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import coerce_region_id_or_city_name_to_region_id

        region_id = coerce_region_id_or_city_name_to_region_id("Sydney")

        print(region_id)
        # abcd1234-ab12-ab12-ab12-abcdef123456
    """
    if is_uuid_format(region_id_or_city_name):
        return str(region_id_or_city_name)
    return str(get_region_obj_from_city_name(region_id_or_city_name).id)


def get_region_obj_from_project_id(
        project_id: Union[UUID4, str]
) -> Region:
    """
    Return the region object associated with a project.

    :param project_id: The project identifier as a UUID4 object or UUID-formatted string

    :return: The region object assigned to the project
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_obj_from_project_id

        region = get_region_obj_from_project_id("abcd1234-ab12-ab12-ab12-abcdef123456")

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
        # Region ID: abcd1234-ab12-ab12-ab12-abcdef123456, City Name: Sydney
    """
    from ...project import get_project_obj_from_project_id
    return get_project_obj_from_project_id(project_id).region


def get_region_from_bundle_id(
        bundle_id: Union[UUID4, str]
) -> Region:
    """
    Return the region object associated with a bundle.

    :param bundle_id: The bundle identifier as a UUID4 object or UUID-formatted string

    :return: The region object assigned to the bundle
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_region_from_bundle_id

        region = get_region_from_bundle_id("abcd1234-ab12-ab12-ab12-abcdef123456")

        print(f"Region ID: {region.id}, City Name: {region.city_name}")
        # Region ID: abcd1234-ab12-ab12-ab12-abcdef123456, City Name: Sydney
    """
    from ...bundle import get_bundle_obj_from_bundle_id
    bundle_obj = get_bundle_obj_from_bundle_id(bundle_id)
    return bundle_obj.region


def set_default_region() -> None:
    """
    Set the default region from the single available region in the tenant.

    :raises Exception: If no regions are found or multiple regions exist

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import set_default_region

        # Sets the global default region for the session
        set_default_region()
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
    Return the default region, setting it automatically if not already set.

    :return: The default region object for the session
    :rtype: `Region <https://umccr.github.io/libica/openapi/v3/docs/Region/>`_

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.region import get_default_region

        region = get_default_region()

        print(f"Default region: {region.city_name}")
        # Default region: Sydney
    """
    if DEFAULT_REGION is None:
        set_default_region()
    return DEFAULT_REGION
