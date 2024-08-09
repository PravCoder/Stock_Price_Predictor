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
import base64
import requests

from hopsworks.client import base, auth, exceptions


class Client(base.Client):
    def __init__(
        self,
        host,
        port,
        project,
        hostname_verification,
        trust_store_path,
        cert_folder,
        api_key_file,
        api_key_value,
    ):
        """Initializes a client in an external environment such as AWS Sagemaker."""
        if not host:
            raise exceptions.ExternalClientError("host")

        self._host = host
        self._port = port
        self._base_url = "https://" + self._host + ":" + str(self._port)
        self._project_name = project

        if api_key_value is not None:
            api_key = api_key_value
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
        else:
            raise exceptions.ExternalClientError(
                "Either api_key_file or api_key_value must be set when connecting to"
                " hopsworks from an external environment."
            )

        self._auth = auth.ApiKeyAuth(api_key)

        self._session = requests.session()
        self._connected = True
        self._verify = self._get_verify(self._host, trust_store_path)

        self._cert_folder_base = os.path.join(cert_folder, host)

    def download_certs(self, project_name):
        project_info = self._get_project_info(project_name)
        project_id = str(project_info["projectId"])

        project_cert_folder = os.path.join(self._cert_folder_base, project_name)

        trust_store_path = os.path.join(project_cert_folder, "trustStore.jks")
        key_store_path = os.path.join(project_cert_folder, "keyStore.jks")

        os.makedirs(project_cert_folder, exist_ok=True)
        credentials = self._get_credentials(project_id)
        self._write_b64_cert_to_bytes(
            str(credentials["kStore"]),
            path=key_store_path,
        )
        self._write_b64_cert_to_bytes(
            str(credentials["tStore"]),
            path=trust_store_path,
        )

        self._write_pem_file(
            credentials["caChain"], self._get_ca_chain_path(project_name)
        )
        self._write_pem_file(
            credentials["clientCert"], self._get_client_cert_path(project_name)
        )
        self._write_pem_file(
            credentials["clientKey"], self._get_client_key_path(project_name)
        )

        with open(os.path.join(project_cert_folder, "material_passwd"), "w") as f:
            f.write(str(credentials["password"]))

    def _close(self):
        """Closes a client and deletes certificates."""
        # TODO: Implement certificate cleanup. Currently do not remove certificates as it may break users using hsfs python ingestion.
        self._connected = False

    def _get_jks_trust_store_path(self):
        return self._trust_store_path

    def _get_jks_key_store_path(self):
        return self._key_store_path

    def _get_ca_chain_path(self, project_name) -> str:
        return os.path.join(self._cert_folder_base, project_name, "ca_chain.pem")

    def _get_client_cert_path(self, project_name) -> str:
        return os.path.join(self._cert_folder_base, project_name, "client_cert.pem")

    def _get_client_key_path(self, project_name) -> str:
        return os.path.join(self._cert_folder_base, project_name, "client_key.pem")

    def _get_project_info(self, project_name):
        """Makes a REST call to hopsworks to get all metadata of a project for the provided project.

        :param project_name: the name of the project
        :type project_name: str
        :return: JSON response with project info
        :rtype: dict
        """
        return self._send_request("GET", ["project", "getProjectInfo", project_name])

    def _write_b64_cert_to_bytes(self, b64_string, path):
        """Converts b64 encoded certificate to bytes file .

        :param b64_string:  b64 encoded string of certificate
        :type b64_string: str
        :param path: path where file is saved, including file name. e.g. /path/key-store.jks
        :type path: str
        """

        with open(path, "wb") as f:
            cert_b64 = base64.b64decode(b64_string)
            f.write(cert_b64)

    def _cleanup_file(self, file_path):
        """Removes local files with `file_path`."""
        try:
            os.remove(file_path)
        except OSError:
            pass

    def replace_public_host(self, url):
        """no need to replace as we are already in external client"""
        return url

    @property
    def host(self):
        return self._host
