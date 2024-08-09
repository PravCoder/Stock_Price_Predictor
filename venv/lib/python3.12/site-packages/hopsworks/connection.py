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

import os
import re
import warnings
import sys

from requests.exceptions import ConnectionError

from hopsworks.decorators import connected, not_connected
from hopsworks import client, version
from hopsworks.core import project_api, secret_api, variable_api

HOPSWORKS_PORT_DEFAULT = 443
HOSTNAME_VERIFICATION_DEFAULT = True
CERT_FOLDER_DEFAULT = "/tmp"
PROJECT_ID = "HOPSWORKS_PROJECT_ID"
PROJECT_NAME = "HOPSWORKS_PROJECT_NAME"


class Connection:
    """A hopsworks connection object.

    This class provides convenience classmethods accessible from the `hopsworks`-module:

    !!! example "Connection factory"
        For convenience, `hopsworks` provides a factory method, accessible from the top level
        module, so you don't have to import the `Connection` class manually:

        ```python
        import hopsworks
        conn = hopsworks.connection()
        ```

    !!! hint "Save API Key as File"
        To get started quickly, you can simply create a file with the previously
         created Hopsworks API Key and place it on the environment from which you
         wish to connect to Hopsworks.

        You can then connect by simply passing the path to the key file when
        instantiating a connection:

        ```python hl_lines="6"
            import hopsworks
            conn = hopsworks.connection(
                'my_instance',                      # DNS of your Hopsworks instance
                443,                                # Port to reach your Hopsworks instance, defaults to 443
                api_key_file='hopsworks.key',       # The file containing the API key generated above
                hostname_verification=True)         # Disable for self-signed certificates
            )
            project = conn.get_project("my_project")
        ```

    Clients in external clusters need to connect to the Hopsworks using an
    API key. The API key is generated inside the Hopsworks platform, and requires at
    least the "project" scope to be able to access a project.
    For more information, see the [integration guides](../setup.md).

    # Arguments
        host: The hostname of the Hopsworks instance, defaults to `None`.
        port: The port on which the Hopsworks instance can be reached,
            defaults to `443`.
        project: The name of the project to connect to. If this is set connection.get_project() will return
        the set project. If not set connection.get_project("my_project") should be used.
        hostname_verification: Whether or not to verify Hopsworks’ certificate, defaults
            to `True`.
        trust_store_path: Path on the file system containing the Hopsworks certificates,
            defaults to `None`.
        cert_folder: The directory to store retrieved HopsFS certificates, defaults to
            `"/tmp"`. Only required to produce messages to Kafka broker from external environment.
        api_key_file: Path to a file containing the API Key.
        api_key_value: API Key as string, if provided, however, this should be used with care,
        especially if the used notebook or job script is accessible by multiple parties. Defaults to `None`.

    # Returns
        `Connection`. Connection handle to perform operations on a
            Hopsworks project.
    """

    def __init__(
        self,
        host: str = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str = None,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        self._host = host
        self._port = port
        self._project = project
        self._hostname_verification = hostname_verification
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder
        self._api_key_file = api_key_file
        self._api_key_value = api_key_value
        self._connected = False

        self.connect()

    @connected
    def get_secrets_api(self):
        """Get the secrets api.

        # Returns
            `SecretsApi`: The Secrets Api handle
        """
        return self._secret_api

    @connected
    def create_project(
        self, name: str, description: str = None, feature_store_topic: str = None
    ):
        """Create a new project.

        Example for creating a new project

        ```python

        import hopsworks

        connection = hopsworks.connection()

        connection.create_project("my_hopsworks_project", description="An example Hopsworks project")

        ```
        # Arguments
            name: The name of the project.
            description: optional description of the project
            feature_store_topic: optional feature store topic name

        # Returns
            `Project`. A project handle object to perform operations on.
        """
        return self._project_api._create_project(name, description, feature_store_topic)

    @connected
    def get_project(self, name: str = None):
        """Get an existing project.

        # Arguments
            name: The name of the project.

        # Returns
            `Project`. A project handle object to perform operations on.
        """

        if not name:
            name = client.get_instance()._project_name

        return self._project_api._get_project(name)

    @connected
    def get_projects(self):
        """Get all projects.

        # Returns
            `List[Project]`: List of Project objects
        """

        return self._project_api._get_projects()

    @connected
    def project_exists(self, name: str):
        """Check if a project exists.

        # Arguments
            name: The name of the project.

        # Returns
            `bool`. True if project exists, otherwise False
        """
        return self._project_api._exists(name)

    @connected
    def _check_compatibility(self):
        """Check the compatibility between the client and backend.
        Assumes versioning (major.minor.patch).
        A client is considered compatible if the major and minor version matches.

        """

        versionPattern = r"\d+\.\d+"
        regexMatcher = re.compile(versionPattern)

        client_version = version.__version__
        backend_version = self._variable_api.get_version("hopsworks")

        major_minor_client = regexMatcher.search(client_version).group(0)
        major_minor_backend = regexMatcher.search(backend_version).group(0)

        if major_minor_backend != major_minor_client:
            print("\n", file=sys.stderr)
            warnings.warn(
                "The installed hopsworks client version {0} may not be compatible with the connected Hopsworks backend version {1}. \nTo ensure compatibility please install the latest bug fix release matching the minor version of your backend ({2}) by running 'pip install hopsworks=={2}.*'".format(
                    client_version, backend_version, major_minor_backend
                )
            )
            sys.stderr.flush()

    def _set_client_variables(self):
        python_version = self._variable_api.get_variable(
            "docker_base_image_python_version"
        )
        client.set_python_version(python_version)

    @not_connected
    def connect(self):
        """Instantiate the connection.

        Creating a `Connection` object implicitly calls this method for you to
        instantiate the connection. However, it is possible to close the connection
        gracefully with the `close()` method, in order to clean up materialized
        certificates. This might be desired when working on external environments such
        as AWS SageMaker. Subsequently you can call `connect()` again to reopen the
        connection.

        !!! example
            ```python
            import hopsworks
            conn = hopsworks.connection()
            conn.close()
            conn.connect()
            ```
        """
        client.stop()
        self._connected = True
        try:
            # init client
            if client.base.Client.REST_ENDPOINT not in os.environ:
                client.init(
                    "external",
                    self._host,
                    self._port,
                    self._project,
                    self._hostname_verification,
                    self._trust_store_path,
                    self._cert_folder,
                    self._api_key_file,
                    self._api_key_value,
                )
            else:
                client.init("hopsworks")

            self._project_api = project_api.ProjectApi()
            self._secret_api = secret_api.SecretsApi()
            self._variable_api = variable_api.VariableApi()
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print(
            "Connected. Call `.close()` to terminate connection gracefully.",
            flush=True,
        )

        self._check_compatibility()
        self._set_client_variables()

    def close(self):
        """Close a connection gracefully.

        This will clean up any materialized certificates on the local file system of
        external environments such as AWS SageMaker.

        Usage is recommended but optional.
        """
        from hsfs import client as hsfs_client
        from hsfs import engine as hsfs_engine
        from hsml import client as hsml_client

        try:
            hsfs_client.stop()
        except:  # noqa: E722
            pass

        try:
            hsfs_engine.stop()
        except:  # noqa: E722
            pass

        try:
            hsml_client.stop()
        except:  # noqa: E722
            pass

        client.stop()
        self._connected = False

        print("Connection closed.")

    @classmethod
    def connection(
        cls,
        host: str = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str = None,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        """Connection factory method, accessible through `hopsworks.connection()`."""
        return cls(
            host,
            port,
            project,
            hostname_verification,
            trust_store_path,
            cert_folder,
            api_key_file,
            api_key_value,
        )

    @property
    def host(self):
        return self._host

    @host.setter
    @not_connected
    def host(self, host):
        self._host = host

    @property
    def port(self):
        return self._port

    @port.setter
    @not_connected
    def port(self, port):
        self._port = port

    @property
    def project(self):
        return self._project

    @project.setter
    @not_connected
    def project(self, project):
        self._project = project

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    @not_connected
    def hostname_verification(self, hostname_verification):
        self._hostname_verification = hostname_verification

    @property
    def api_key_file(self):
        return self._api_key_file

    @property
    def api_key_value(self):
        return self._api_key_value

    @api_key_file.setter
    @not_connected
    def api_key_file(self, api_key_file):
        self._api_key_file = api_key_file

    @api_key_value.setter
    @not_connected
    def api_key_value(self, api_key_value):
        self._api_key_value = api_key_value

    @property
    def trust_store_path(self):
        return self._trust_store_path

    @trust_store_path.setter
    @not_connected
    def trust_store_path(self, trust_store_path):
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self):
        return self._cert_folder

    @cert_folder.setter
    @not_connected
    def cert_folder(self, cert_folder):
        self._cert_folder = cert_folder

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
