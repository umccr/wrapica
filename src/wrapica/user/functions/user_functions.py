#!/usr/bin/env python3

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


def get_user_obj_from_user_id(user_id: str) -> User:
    """
    Get the user object from the user id

    :param user_id:

    :return: `User <https://umccr-illumina.github.io/libica/openapi/v2/docs/User/>`_
    """
    with ApiClient(get_icav2_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = UserApi(api_client)

    # example passing only required values which don't have defaults set
    try:
        # Retrieve a user.
        api_response: User = api_instance.get_user(user_id)
    except ApiException as e:
        logger.error("Exception when calling UserApi->get_user: %s\n" % e)
        raise ApiException

    return api_response


def get_user_name_from_user_id(user_id: str) -> str:
    """
    Get the user name from the user id

    :param user_id: The user id

    :return: The user name
    :rtype: str
    """
    return get_user_obj_from_user_id(user_id).username


def get_user_obj_from_user_name(user_name: str) -> User:
    """
    Get the user object from the user name
    Lists through all user objects, when we have both user id and user name, use get_user_from_user_id instead

    :param user_name:

    :return: The user object
    :rtype: `User <https://umccr-illumina.github.io/libica/openapi/v2/docs/User/>`_

    :raises ApiException: If an error occurs when collecting the users
    :raises ValueError: If the user name is not found
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
    Given either a user id or name, coerce to user object

    :param user_id_or_user_name:

    :return: The user object
    :rtype: `User <https://umccr-illumina.github.io/libica/openapi/v2/docs/User/>`_

    :raises ValueError: If the user name is not found
    :raises ApiException: If an error occurs when collecting the users

    :Example:

    .. code-block:: python
        :linenos:

        my_user: User = coerce_user_id_or_user_name_to_user_obj("user_id")
    """
    if is_uuid_format(user_id_or_user_name):
        return get_user_obj_from_user_id(user_id_or_user_name)

    return get_user_obj_from_user_name(user_id_or_user_name)


def coerce_user_id_or_name_to_user_id(user_id_or_user_name: str) -> str:
    """
    Given either a user id or name, coerce to user id

    :param user_id_or_user_name:

    :return: The user id
    """
    if is_uuid_format(user_id_or_user_name):
        return user_id_or_user_name

    return get_user_obj_from_user_name(user_id_or_user_name).id


def get_user_id_from_configuration() -> str:
    """
    Use jwt to get username from access token

    :return: The user id
    :rtype: str
    """
    return get_jwt_token_obj(get_icav2_configuration().access_token, ICAV2_ACCESS_TOKEN_AUDIENCE).get("sub")


def get_tenant_id_for_user():
    """
    Get user and return tenant id from user

    :return: The tenant id
    :rtype: str
    """
    user_id = get_user_id_from_configuration()

    return get_user_obj_from_user_id(user_id).tenant_id
