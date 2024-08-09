#
#   Copyright 2022 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import warnings
import logging
import os
import sys
import getpass
import tempfile
from pathlib import Path

from hopsworks.client.exceptions import RestAPIError, ProjectException
from hopsworks import version, constants, client
from hopsworks.connection import Connection

# Needs to run before import of hsml and hsfs
warnings.filterwarnings(action="ignore", category=UserWarning, module=r".*psycopg2")

import hsml  # noqa: F401, E402
import hsfs  # noqa: F401, E402

__version__ = version.__version__

connection = Connection.connection

_hw_connection = Connection.connection

_connected_project = None


def hw_formatwarning(message, category, filename, lineno, line=None):
    return "{}: {}\n".format(category.__name__, message)


warnings.formatwarning = hw_formatwarning

__all__ = ["connection"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)


def login(
    host: str = None,
    port: int = 443,
    project: str = None,
    api_key_value: str = None,
    api_key_file: str = None,
):
    """Connect to [Serverless Hopsworks](https://app.hopsworks.ai) by calling the `hopsworks.login()` function with no arguments.

    ```python

    project = hopsworks.login()

    ```

    Alternatively, connect to your own Hopsworks installation by specifying the host, port and api key.

    ```python

    project = hopsworks.login(host="my.hopsworks.server",
                              port=8181,
                              api_key_value="DKN8DndwaAjdf98FFNSxwdVKx")

    ```

    In addition to setting function arguments directly, `hopsworks.login()` also reads the environment variables:
    HOPSWORKS_HOST, HOPSWORKS_PORT, HOPSWORKS_PROJECT and HOPSWORKS_API_KEY.

    The function arguments do however take precedence over the environment variables in case both are set.

    # Arguments
        host: The hostname of the Hopsworks instance, defaults to `None`.
        port: The port on which the Hopsworks instance can be reached,
            defaults to `443`.
        project: Name of the project to access. If used inside a Hopsworks environment it always gets the current project. If not provided you will be prompted to enter it.
        api_key_value: Value of the Api Key
        api_key_file: Path to file wih Api Key
    # Returns
        `Project`: The Project object
    # Raises
        `RestAPIError`: If unable to connect to Hopsworks
    """

    global _connected_project

    # If already logged in, should reset connection and follow login procedure as Connection may no longer be valid
    logout()

    global _hw_connection

    # If inside hopsworks, just return the current project for now
    if "REST_ENDPOINT" in os.environ:
        _hw_connection = _hw_connection()
        _connected_project = _hw_connection.get_project()
        print("\nLogged in to project, explore it here " + _connected_project.get_url())
        return _connected_project

    # This is run for an external client

    # Function arguments takes precedence over environment variable.
    # Here we check if environment variable exists and function argument is not set, we use the environment variable.

    # If api_key_value/api_key_file not defined, get HOPSWORKS_API_KEY environment variable
    api_key = None
    if (
        api_key_value is None
        and api_key_file is None
        and "HOPSWORKS_API_KEY" in os.environ
    ):
        api_key = os.environ["HOPSWORKS_API_KEY"]

    # If project argument not defined, get HOPSWORKS_PROJECT environment variable
    if project is None and "HOPSWORKS_PROJECT" in os.environ:
        project = os.environ["HOPSWORKS_PROJECT"]

    # If host argument not defined, get HOPSWORKS_HOST environment variable
    if host is None and "HOPSWORKS_HOST" in os.environ:
        host = os.environ["HOPSWORKS_HOST"]
    elif host is None:  # Always do a fallback to Serverless Hopsworks if not defined
        host = constants.HOSTS.APP_HOST

    # If port same as default, get HOPSWORKS_HOST environment variable
    if port == 443 and "HOPSWORKS_PORT" in os.environ:
        port = os.environ["HOPSWORKS_PORT"]

    # This .hw_api_key is created when a user logs into Serverless Hopsworks the first time.
    # It is then used only for future login calls to Serverless. For other Hopsworks installations it's ignored.
    api_key_path = _get_cached_api_key_path()

    # Conditions for getting the api_key
    # If user supplied the api key directly
    if api_key_value is not None:
        api_key = api_key_value
    # If user supplied the api key in a file
    elif api_key_file is not None:
        file = None
        if os.path.exists(api_key_file):
            try:
                file = open(api_key_file, mode="r")
                api_key = file.read()
            finally:
                file.close()
        else:
            raise IOError(
                "Could not find api key file on path: {}".format(api_key_file)
            )
    # If user connected to Serverless Hopsworks, and the cached .hw_api_key exists, then use it.
    elif os.path.exists(api_key_path) and host == constants.HOSTS.APP_HOST:
        try:
            _hw_connection = _hw_connection(
                host=host, port=port, api_key_file=api_key_path
            )
            _connected_project = _prompt_project(_hw_connection, project)
            print(
                "\nLogged in to project, explore it here "
                + _connected_project.get_url()
            )
            return _connected_project
        except RestAPIError:
            logout()
            # API Key may be invalid, have the user supply it again
            os.remove(api_key_path)

    if api_key is None and host == constants.HOSTS.APP_HOST:
        print(
            "Copy your Api Key (first register/login): https://c.app.hopsworks.ai/account/api/generated"
        )
        api_key = getpass.getpass(prompt="\nPaste it here: ")

        # If api key was provided as input, save the API key locally on disk to avoid users having to enter it again in the same environment
        Path(os.path.dirname(api_key_path)).mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            path=api_key_path, flags=(os.O_WRONLY | os.O_CREAT | os.O_TRUNC), mode=0o600
        )
        with open(descriptor, "w") as fh:
            fh.write(api_key.strip())

    try:
        _hw_connection = _hw_connection(host=host, port=port, api_key_value=api_key)
        _connected_project = _prompt_project(_hw_connection, project)
    except RestAPIError as e:
        logout()
        raise e

    print("\nLogged in to project, explore it here " + _connected_project.get_url())
    return _connected_project


def _get_cached_api_key_path():
    """
    This function is used to get an appropriate path to store the user supplied API Key for Serverless Hopsworks.

    First it will search for .hw_api_key in the current working directory, if it exists it will use it (this is default in 3.0 client)
    Otherwise, falls back to storing the API key in HOME
    If not sufficient permissions are set in HOME to create the API key (writable and executable), it uses the temp directory to store it.

    """

    api_key_name = ".hw_api_key"
    api_key_folder = ".{}_hopsworks_app".format(getpass.getuser())

    # Path for current working directory api key
    cwd_api_key_path = os.path.join(os.getcwd(), api_key_name)

    # Path for home api key
    home_dir_path = os.path.expanduser("~")
    home_api_key_path = os.path.join(home_dir_path, api_key_folder, api_key_name)

    # Path for tmp api key
    temp_dir_path = tempfile.gettempdir()
    temp_api_key_path = os.path.join(temp_dir_path, api_key_folder, api_key_name)

    if os.path.exists(
        cwd_api_key_path
    ):  # For backward compatibility, if .hw_api_key exists in current working directory get it from there
        api_key_path = cwd_api_key_path
    elif os.access(home_dir_path, os.W_OK) and os.access(
        home_dir_path, os.X_OK
    ):  # Otherwise use the home directory of the user
        api_key_path = home_api_key_path
    else:  # Finally try to store it in temp
        api_key_path = temp_api_key_path

    return api_key_path


def _prompt_project(valid_connection, project):
    saas_projects = valid_connection.get_projects()
    if project is None:
        if len(saas_projects) == 0:
            raise ProjectException("Could not find any project")
        elif len(saas_projects) == 1:
            return saas_projects[0]
        else:
            while True:
                print("\nMultiple projects found. \n")
                for index in range(len(saas_projects)):
                    print("\t (" + str(index + 1) + ") " + saas_projects[index].name)
                while True:
                    project_index = input("\nEnter project to access: ")
                    # Handle invalid input type
                    try:
                        project_index = int(project_index)
                        # Handle negative indexing
                        if project_index <= 0:
                            print("Invalid input, must be greater than or equal to 1")
                            continue
                        # Handle index out of range
                        try:
                            return saas_projects[project_index - 1]
                        except IndexError:
                            print(
                                "Invalid input, should be an integer from the list of projects."
                            )
                    except ValueError:
                        print(
                            "Invalid input, should be an integer from the list of projects."
                        )
    else:
        for proj in saas_projects:
            if proj.name == project:
                return proj
        raise ProjectException("Could not find project {}".format(project))


def logout():
    global _hw_connection
    if isinstance(_hw_connection, Connection):
        _hw_connection.close()
    client.stop()
    _hw_connection = Connection.connection
