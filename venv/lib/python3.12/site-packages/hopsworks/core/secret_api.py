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

from hopsworks import client, secret
from hopsworks.core import project_api
import json


class SecretsApi:
    def __init__(
        self,
    ):
        self._project_api = project_api.ProjectApi()

    def get_secrets(self):
        """Get all secrets

        # Returns
            `List[Secret]`: List of all accessible secrets
        # Raises
            `RestAPIError`: If unable to get the secrets
        """
        _client = client.get_instance()
        path_params = [
            "users",
            "secrets",
        ]
        return secret.Secret.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_secret(self, name: str, owner: str = None):
        """Get a secret.

        # Arguments
            name: Name of the project.
            owner: email of the owner for a secret shared with the current project.
        # Returns
            `Secret`: The Secret object
        # Raises
            `RestAPIError`: If unable to get the secret
        """
        _client = client.get_instance()
        query_params = None
        if owner is None:
            path_params = [
                "users",
                "secrets",
                name,
            ]
        else:
            query_params = {"name": name, "owner": owner}
            path_params = [
                "users",
                "secrets",
                "shared",
            ]

        return secret.Secret.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )[0]

    def create_secret(self, name: str, value: str, project: str = None):
        """Create a new secret.

        ```python

        import hopsworks

        connection = hopsworks.connection()

        secrets_api = connection.get_secrets_api()

        secret = secrets_api.create_secret("my_secret", "Fk3MoPlQXCQvPo")

        ```
        # Arguments
            name: Name of the secret.
            value: The secret value.
            project: Name of the project to share the secret with.
        # Returns
            `Secret`: The Secret object
        # Raises
            `RestAPIError`: If unable to create the secret
        """
        _client = client.get_instance()

        secret_config = {"name": name, "secret": value}

        if project is None:
            secret_config["visibility"] = "PRIVATE"
        else:
            scope_project = self._project_api._get_project(project)
            secret_config["scope"] = scope_project.id
            secret_config["visibility"] = "PROJECT"

        path_params = [
            "users",
            "secrets",
        ]

        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(secret_config)
        )

        created_secret = self.get_secret(name)
        print(created_secret.get_url())
        return created_secret

    def _delete(self, name: str):
        """Delete the secret.
        :param name: name of the secret
        :type name: Secret
        """
        _client = client.get_instance()
        path_params = [
            "users",
            "secrets",
            name,
        ]
        _client._send_request("DELETE", path_params)
