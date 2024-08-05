#
#   Copyright 2021 Logical Clocks AB
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

import requests

from hsml.client import auth, exceptions
from hsml.client.hopsworks import base as hopsworks


class Client(hopsworks.Client):
    def __init__(
        self,
        host,
        port,
        project,
        hostname_verification,
        trust_store_path,
        api_key_file,
        api_key_value,
    ):
        """Initializes a client in an external environment."""
        if not host:
            raise exceptions.ExternalClientError(
                "host cannot be of type NoneType, host is a non-optional "
                "argument to connect to hopsworks from an external environment."
            )
        if not project:
            raise exceptions.ExternalClientError(
                "project cannot be of type NoneType, project is a non-optional "
                "argument to connect to hopsworks from an external environment."
            )

        self._host = host
        self._port = port
        self._base_url = "https://" + self._host + ":" + str(self._port)
        self._project_name = project

        api_key = auth.get_api_key(api_key_value, api_key_file)
        self._auth = auth.ApiKeyAuth(api_key)

        self._session = requests.session()
        self._connected = True
        self._verify = self._get_verify(self._host, trust_store_path)

        if self._project_name is not None:
            project_info = self._get_project_info(self._project_name)
            self._project_id = str(project_info["projectId"])
        else:
            self._project_id = None

        self._cert_key = None

    def _close(self):
        """Closes a client."""
        self._connected = False

    def _get_project_info(self, project_name):
        """Makes a REST call to hopsworks to get all metadata of a project for the provided project.

        :param project_name: the name of the project
        :type project_name: str
        :return: JSON response with project info
        :rtype: dict
        """
        return self._send_request("GET", ["project", "getProjectInfo", project_name])

    def replace_public_host(self, url):
        """no need to replace as we are already in external client"""
        return url

    @property
    def host(self):
        return self._host
