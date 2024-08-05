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
from abc import abstractmethod

from hsml.client import base, auth


class Client(base.Client):
    TOKEN_FILE = "token.jwt"
    APIKEY_FILE = "api.key"
    REST_ENDPOINT = "REST_ENDPOINT"
    HOPSWORKS_PUBLIC_HOST = "HOPSWORKS_PUBLIC_HOST"

    BASE_PATH_PARAMS = ["hopsworks-api", "api"]

    @abstractmethod
    def __init__(self):
        """To be extended by clients."""
        pass

    def _get_verify(self, verify, trust_store_path):
        """Get verification method for sending HTTP requests to Hopsworks.

        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c

        :param verify: perform hostname verification, 'true' or 'false'
        :type verify: str
        :param trust_store_path: path of the truststore locally if it was uploaded manually to
            the external environment
        :type trust_store_path: str
        :return: if verify is true and the truststore is provided, then return the trust store location
                 if verify is true but the truststore wasn't provided, then return true
                 if verify is false, then return false
        :rtype: str or boolean
        """
        if verify == "true":
            if trust_store_path is not None:
                return trust_store_path
            else:
                return True

        return False

    def _get_retry(self, request, response):
        """Get retry method for resending HTTP requests to Hopsworks

        :param request: original HTTP request already sent
        :type request: requests.Request
        :param response: response of the original HTTP request
        :type response: requests.Response
        """
        if response.status_code == 401 and self.REST_ENDPOINT in os.environ:
            # refresh token and retry request - only on hopsworks
            self._auth = auth.BearerAuth(self._read_jwt())
            # Update request with the new token
            request.auth = self._auth
            # retry request
            return True
        return False

    def _get_host_port_pair(self):
        """
        Removes "http or https" from the rest endpoint and returns a list
        [endpoint, port], where endpoint is on the format /path.. without http://

        :return: a list [endpoint, port]
        :rtype: list
        """
        endpoint = self._base_url
        if endpoint.startswith("http"):
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _read_jwt(self):
        """Retrieve jwt from local container."""
        return self._read_file(self.TOKEN_FILE)

    def _read_apikey(self):
        """Retrieve apikey from local container."""
        return self._read_file(self.APIKEY_FILE)

    def _read_file(self, secret_file):
        """Retrieve secret from local container."""
        with open(os.path.join(self._secrets_dir, secret_file), "r") as secret:
            return secret.read()

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False

    def replace_public_host(self, url):
        """replace hostname to public hostname set in HOPSWORKS_PUBLIC_HOST"""
        ui_url = url._replace(netloc=os.environ[self.HOPSWORKS_PUBLIC_HOST])
        return ui_url
