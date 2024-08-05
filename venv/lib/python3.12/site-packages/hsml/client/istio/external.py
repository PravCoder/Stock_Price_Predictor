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

import requests

from hsml.client import auth
from hsml.client.istio import base as istio


class Client(istio.Client):
    def __init__(
        self,
        host,
        port,
        project,
        api_key_value,
        hostname_verification=None,
        trust_store_path=None,
    ):
        """Initializes a client in an external environment such as AWS Sagemaker."""
        self._host = host
        self._port = port
        self._base_url = "http://" + self._host + ":" + str(self._port)
        self._project_name = project

        self._auth = auth.ApiKeyAuth(api_key_value)

        self._session = requests.session()
        self._connected = True
        self._verify = self._get_verify(hostname_verification, trust_store_path)

        self._cert_key = None

    def _close(self):
        """Closes a client."""
        self._connected = False

    def replace_public_host(self, url):
        """no need to replace as we are already in external client"""
        return url

    @property
    def host(self):
        return self._host
