#!/usr/bin/env python3

# Standard library imports
from typing import Union
from pydantic import UUID4

# Libica Api imports
from libica.openapi.v3.api.user_api import UserApi
from libica.openapi.v3 import ApiClient, ApiException

# Libica model imports
from libica.openapi.v3.models import (
    User,
    UserList
)

# Local imports
from ...utils.globals import ICAV2_ACCESS_TOKEN_AUDIENCE
from ...utils.logger import get_logger
from ...utils.configuration import get_icav2_configuration, get_jwt_token_obj
from ...utils.miscell import is_uuid_format

# Get logger
logger = get_logger()


def get_user_obj_from_user_id(
        user_id: Union[UUID4, str]
) -> User:
    """
    Retrieve the user object for a given user identifier.

    :param user_id: The user identifier as a UUID4 object or UUID-formatted string

    :return: The user object matching the specified identifier
    :rtype: `User <https://umccr.github.io/libica/openapi/v3/docs/User/>`_

    :raises ApiException: If the API call to retrieve the user fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import get_user_obj_from_user_id

        user = get_user_obj_from_user_id("user-1234-abcd-5678")

        print(f"Username: {user.username}")
        # Username: jsmith
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = UserApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a user.
        api_response: User = api_instance.get_user(user_id=user_id)
    except ApiException as e:
        logger.error("Exception when calling UserApi->get_user: %s\n" % e)
        raise ApiException

    return api_response


def get_user_name_from_user_id(
        user_id: Union[UUID4, str]
) -> str:
    """
    Return the username string for a given user identifier.

    :param user_id: The user identifier as a UUID4 object or UUID-formatted string

    :return: The username associated with the user identifier
    :rtype: str

    :raises ApiException: If the API call to retrieve the user fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import get_user_name_from_user_id

        username = get_user_name_from_user_id("user-1234-abcd-5678")

        print(f"Username: {username}")
        # Username: jsmith
    """
    return get_user_obj_from_user_id(user_id).username


def get_user_obj_from_user_name(user_name: str) -> User:
    """
    Retrieve the user object matching a given full name.

    :param user_name: The user full name as firstname and lastname joined by a space

    :return: The user object whose full name matches the specified name
    :rtype: `User <https://umccr.github.io/libica/openapi/v3/docs/User/>`_

    :raises ApiException: If the API call to list users fails
    :raises ValueError: If no user with the specified name is found

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import get_user_obj_from_user_name

        user = get_user_obj_from_user_name("Jane Doe")

        print(f"User ID: {user.id}")
        # User ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = UserApi(api_client)

    # Get the user
    try:
        # Retrieve a user.
        api_response: UserList = api_instance.get_users()
    except ApiException as e:
        raise ApiException("Exception when calling UserApi->get_user: %s\n" % e)

    try:
        return next(
            filter(
                lambda x: x.firstname + " " + x.lastname == user_name,
                api_response.items
            )
        )
    except StopIteration:
        logger.error(f"Could not find user name '{user_name}'")
        raise ValueError


def coerce_user_id_or_name_to_user_obj(user_id_or_user_name: str) -> User:
    """
    Coerce a user identifier or full name to a user object.

    :param user_id_or_user_name: The user identifier in UUID format or the user full name

    :return: The user object resolved from the identifier or name
    :rtype: `User <https://umccr.github.io/libica/openapi/v3/docs/User/>`_

    :raises ValueError: If no user with the specified name is found
    :raises ApiException: If the API call to retrieve or list users fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import coerce_user_id_or_name_to_user_obj

        user = coerce_user_id_or_name_to_user_obj("user-1234-abcd-5678")

        print(f"Username: {user.username}")
        # Username: jsmith

    .. code-block:: python
        :linenos:

        from wrapica.user import coerce_user_id_or_name_to_user_obj

        user = coerce_user_id_or_name_to_user_obj("Jane Doe")

        print(f"User ID: {user.id}")
        # User ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    if is_uuid_format(user_id_or_user_name):
        return get_user_obj_from_user_id(user_id_or_user_name)

    return get_user_obj_from_user_name(user_id_or_user_name)


def coerce_user_id_or_name_to_user_id(user_id_or_user_name: str) -> str:
    """
    Coerce a user identifier or full name to a user identifier string.

    :param user_id_or_user_name: The user identifier in UUID format or the user full name

    :return: The user identifier as a UUID-formatted string
    :rtype: str

    :raises ValueError: If no user with the specified name is found
    :raises ApiException: If the API call to list users fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import coerce_user_id_or_name_to_user_id

        user_id = coerce_user_id_or_name_to_user_id("Jane Doe")

        print(f"User ID: {user_id}")
        # User ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    if is_uuid_format(user_id_or_user_name):
        return user_id_or_user_name

    return str(get_user_obj_from_user_name(user_id_or_user_name).id)


def get_user_id_from_configuration() -> str:
    """
    Extract the user identifier from the current access token configuration.

    :return: The user identifier extracted from the JWT access token
    :rtype: str

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import get_user_id_from_configuration

        user_id = get_user_id_from_configuration()

        print(f"Current user ID: {user_id}")
        # Current user ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    return get_jwt_token_obj(get_icav2_configuration().access_token, ICAV2_ACCESS_TOKEN_AUDIENCE).get("sub")


def get_tenant_id_for_user() -> str:
    """
    Return the tenant identifier for the currently authenticated user.

    :return: The tenant identifier associated with the current user
    :rtype: str

    :raises ApiException: If the API call to retrieve the user fails

    :Examples:

    .. code-block:: python
        :linenos:

        from wrapica.user import get_tenant_id_for_user

        tenant_id = get_tenant_id_for_user()

        print(f"Tenant ID: {tenant_id}")
        # Tenant ID: abcd1234-ab12-ab12-ab12-abcdef123456
    """
    user_id = get_user_id_from_configuration()

    return get_user_obj_from_user_id(user_id).tenant_id
