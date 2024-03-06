#!/usr/bin/env python3

"""
Intialise the configuration for the application
"""
# Standard imports
from datetime import datetime
from pathlib import Path
from os import environ
from typing import Optional, OrderedDict
from urllib.parse import urlparse
from jwt import decode, InvalidTokenError
from ruamel.yaml import YAML

# Libica imports
from libica.openapi.v2 import Configuration

# Local imports
from .globals import ICAV2_CONFIG_FILE_PATH, ICAV2_CONFIG_FILE_SERVER_URL_KEY, DEFAULT_ICAV2_BASE_URL, \
    ICAV2_ACCESS_TOKEN_AUDIENCE, ICAV2_SESSION_FILE_PATH, ICAV2_SESSION_FILE_ACCESS_TOKEN_KEY
from .logger import get_logger
from .subprocess_handler import run_subprocess_proc

# Set logger
logger = get_logger()

# Global runtime vars
ICAV2_CONFIGURATION: Optional[Configuration] = None


# Read the configuration file
def get_config_file_path() -> Path:
    """
    Get path for the config file and asser it exists
    :return:
    """
    config_file_path: Path = Path(ICAV2_CONFIG_FILE_PATH.format(
      HOME=environ["HOME"]
    ))

    if not config_file_path.is_file():
        logger.error(f"Could not get file path {config_file_path}")
        raise FileNotFoundError

    return config_file_path


def read_config_file() -> OrderedDict:
    """
    Get the contents of the session file (~/.icav2/config.ica.yaml)
    :return:
    """

    logger.debug("Reading in the config file")
    yaml = YAML()

    with open(get_config_file_path(), "r") as file_h:
        data = yaml.load(file_h)

    return data


# Set the icav2 environment variables
def get_icav2_base_url():
    """
    Collect the icav2 base url for the configuration
    Likely 'https://ica.illumina.com/ica/rest'
    :return:
    """
    # Check env
    icav2_base_url_env = environ.get("ICAV2_BASE_URL", None)
    if icav2_base_url_env is not None:
        return icav2_base_url_env

    # Read config file
    config_yaml_dict = read_config_file()
    if ICAV2_CONFIG_FILE_SERVER_URL_KEY in config_yaml_dict.keys():
        return f"https://{config_yaml_dict[ICAV2_CONFIG_FILE_SERVER_URL_KEY]}/ica/rest"
    else:
        logger.warning("Could not get server-url from config yaml")

    return DEFAULT_ICAV2_BASE_URL


def refresh_access_token() -> str:
    """
    Run standard command to get a new token in the session file
    :return:
    """
    project_list_returncode, project_list_stdout, project_list_stderr = \
        run_subprocess_proc(["icav2", "projects", "list"], capture_output=True)

    if not project_list_returncode == 0:
        logger.error("Couldn't run a simple icav2 command to refresh the token")
        raise ChildProcessError

    # Get the newly refreshed token
    return get_access_token_from_session_file(refresh=False)


def check_access_token_expiry(access_token: str) -> bool:
    """
    Check access token hasn't expired
    True if has not expired, otherwise false
    :param access_token:
    :return:
    """
    current_epoch_time = int(datetime.now().strftime("%s"))

    if current_epoch_time < get_jwt_token_obj(access_token, ICAV2_ACCESS_TOKEN_AUDIENCE).get("exp"):
        return True

    # Otherwise
    logger.info("Token has expired")
    return False


def get_session_file_path() -> Path:
    """
    Get path for session file and assert file exists
    :return:
    """
    session_file_path: Path = Path(ICAV2_SESSION_FILE_PATH.format(
        HOME=environ["HOME"],
        server_url_prefix=urlparse(get_icav2_base_url()).netloc.split(".")[0]
    ))

    if not session_file_path.is_file():
        logger.error(f"Could not get file path {session_file_path}")
        raise FileNotFoundError

    return session_file_path


def read_session_file() -> OrderedDict:
    """
    Get the contents of the session file (~/.icav2/.session.ica.yaml)
    :return:
    """

    logger.debug("Reading in the session file")
    yaml = YAML()

    with open(get_session_file_path(), "r") as file_h:
        data = yaml.load(file_h)

    return data


def get_access_token_from_session_file(refresh: bool = True) -> str:
    """
    Collect the contents of the access token
    :return:
    """
    session_data: OrderedDict = read_session_file()

    access_token: str = session_data.get(ICAV2_SESSION_FILE_ACCESS_TOKEN_KEY, None)

    if access_token is None:
        logger.error("Could not get access token from session file")
        raise KeyError

    if not check_access_token_expiry(access_token):
        if not refresh:
            logger.error(f"Could not refresh access token in session file {ICAV2_SESSION_FILE_PATH}")
            raise KeyError
        else:
            access_token = refresh_access_token()

    return access_token


def get_icav2_access_token() -> str:
    """
    Return icav2 access token
    """
    access_token = environ.get("ICAV2_ACCESS_TOKEN", None)

    if access_token is None or not check_access_token_expiry(access_token):
        access_token = get_access_token_from_session_file()

    return access_token


def set_icav2_configuration():
    global ICAV2_CONFIGURATION

    ICAV2_CONFIGURATION = Configuration(
        host=get_icav2_base_url(),
        access_token=get_icav2_access_token()
    )


def get_icav2_configuration() -> Configuration:
    """
    Return icav2 configuration, if not set, sets it first, then returns
    :return:
    """
    if ICAV2_CONFIGURATION is None:
        set_icav2_configuration()
    return ICAV2_CONFIGURATION


def get_jwt_token_obj(jwt_token, audience):
    """
    Get the jwt token object through the pyjwt package
    :param jwt_token: The jwt token in base64url format
    :param audience: The Audience to use for the token, defaults to 'ica'
    :return:
    """
    try:
        token_object = decode(jwt_token,
                              options={
                                         "verify_signature": False,
                                         "require_exp": True
                                       },
                              audience=[audience],
                              algorithms="RS256",
                              )
    except InvalidTokenError:
        raise InvalidTokenError

    return token_object

