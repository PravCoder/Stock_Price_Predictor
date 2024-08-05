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

import furl
from abc import ABC, abstractmethod

import requests
import urllib3

from hsml.client import exceptions
from hsml.decorators import connected


urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Client(ABC):
    @abstractmethod
    def __init__(self):
        """To be implemented by clients."""
        pass

    @abstractmethod
    def _get_verify(self, verify, trust_store_path):
        """To be implemented by clients."""
        pass

    @abstractmethod
    def _get_retry(self, session, request, response):
        """To be implemented by clients."""
        pass

    @abstractmethod
    def _get_host_port_pair(self):
        """To be implemented by clients."""
        pass

    @connected
    def _send_request(
        self,
        method,
        path_params,
        query_params=None,
        headers=None,
        data=None,
        stream=False,
        files=None,
    ):
        """Send REST request to a REST endpoint.

        Uses the client it is executed from. Path parameters are url encoded automatically.

        :param method: 'GET', 'PUT' or 'POST'
        :type method: str
        :param path_params: a list of path params to build the query url from starting after
            the api resource, for example `["project", 119]`.
        :type path_params: list
        :param query_params: A dictionary of key/value pairs to be added as query parameters,
            defaults to None
        :type query_params: dict, optional
        :param headers: Additional header information, defaults to None
        :type headers: dict, optional
        :param data: The payload as a python dictionary to be sent as json, defaults to None
        :type data: dict, optional
        :param stream: Set if response should be a stream, defaults to False
        :type stream: boolean, optional
        :param files: dictionary for multipart encoding upload
        :type files: dict, optional
        :raises RestAPIError: Raised when request wasn't correctly received, understood or accepted
        :return: Response json
        :rtype: dict
        """
        f_url = furl.furl(self._base_url)
        f_url.path.segments = self.BASE_PATH_PARAMS + path_params
        url = str(f_url)
        request = requests.Request(
            method,
            url=url,
            headers=headers,
            data=data,
            params=query_params,
            auth=self._auth,
            files=files,
        )

        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, verify=self._verify, stream=stream)

        if self._get_retry(request, response):
            prepped = self._session.prepare_request(request)
            response = self._session.send(prepped, verify=self._verify, stream=stream)

        if response.status_code // 100 != 2:
            raise exceptions.RestAPIError(url, response)

        if stream:
            return response
        else:
            # handle different success response codes
            if len(response.content) == 0:
                return None
            return response.json()

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False
