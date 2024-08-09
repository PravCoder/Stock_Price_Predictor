#
#   Copyright 2022 Hopsworks AB
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

from hopsworks import client, environment
from hopsworks.engine import environment_engine


class EnvironmentApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

        self._environment_engine = environment_engine.EnvironmentEngine(project_id)

    def create_environment(self, await_creation=True):
        """Create Python environment for the project

        ```python

        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        env = env_api.create_environment()

        ```
        # Arguments
            await_creation: bool. If True the method returns only when the creation is finished. Default True
        # Returns
            `Environment`: The Environment object
        # Raises
            `RestAPIError`: If unable to create the environment
        """
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            client.get_python_version(),
        ]
        headers = {"content-type": "application/json"}
        env = environment.Environment.from_response_json(
            _client._send_request("POST", path_params, headers=headers),
            self._project_id,
            self._project_name,
        )

        if await_creation:
            self._environment_engine.await_environment_command()

        return env

    def _get_environments(self):
        """
        Get all available python environments in the project
        """
        _client = client.get_instance()

        path_params = ["project", self._project_id, "python", "environments"]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            ),
            self._project_id,
            self._project_name,
        )

    def get_environment(self):
        """Get handle for the Python environment for the project

        ```python

        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        env = env_api.get_environment()

        ```
        # Returns
            `Environment`: The Environment object
        # Raises
            `RestAPIError`: If unable to get the environment
        """
        project_envs = self._get_environments()
        if len(project_envs) == 0:
            return None
        elif len(project_envs) > 0:
            return project_envs[0]

    def _delete(self, python_version):
        """Delete the project Python environment"""
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            python_version,
        ]
        headers = {"content-type": "application/json"}
        _client._send_request("DELETE", path_params, headers=headers),
