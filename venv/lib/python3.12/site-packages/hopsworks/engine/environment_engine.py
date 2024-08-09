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

import time

from hopsworks import client, library, environment, command
from hopsworks.client.exceptions import RestAPIError, EnvironmentException


class EnvironmentEngine:
    def __init__(self, project_id):
        self._project_id = project_id

    def await_library_command(self, library_name=None):
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            time.sleep(5)
            library = self._poll_commands_library(library_name)
            if library is None:
                commands = []
            else:
                commands = library._commands

    def await_environment_command(self):
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            time.sleep(5)
            environment = self._poll_commands_environment()
            if environment is None:
                commands = []
            else:
                commands = environment._commands

    def _is_final_status(self, command):
        if command.status == "FAILED":
            raise EnvironmentException(
                "Command failed with stacktrace: \n{}".format(command.error_message)
            )
        elif command.status == "SUCCESS":
            return True
        else:
            return False

    def _poll_commands_library(self, library_name):
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            client.get_python_version(),
            "libraries",
            library_name,
        ]

        query_params = {"expand": "commands"}
        headers = {"content-type": "application/json"}

        try:
            return library.Library.from_response_json(
                _client._send_request(
                    "GET", path_params, headers=headers, query_params=query_params
                ),
                None,
                None,
            )
        except RestAPIError as e:
            if (
                e.response.json().get("errorCode", "") == 300003
                and e.response.status_code == 404
            ):
                return None

    def _poll_commands_environment(self):
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            client.get_python_version(),
        ]

        query_params = {"expand": "commands"}
        headers = {"content-type": "application/json"}

        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            None,
            None,
        )
