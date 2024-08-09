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

import humps

from hopsworks import command


class Library:
    def __init__(
        self,
        channel,
        package_source,
        library,
        version,
        latest_version=None,
        status=None,
        preinstalled=None,
        commands=None,
        type=None,
        href=None,
        environment=None,
        project_id=None,
        **kwargs,
    ):
        self._channel = channel
        self._package_source = package_source
        self._library = library
        self._version = version
        self._latest_version = latest_version
        self._status = status
        self._preinstalled = preinstalled
        self._commands = (
            command.Command.from_response_json(commands) if commands else None
        )

        self._environment = environment
        self._project_id = project_id

    @classmethod
    def from_response_json(cls, json_dict, environment, project_id):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized, environment=environment, project_id=project_id)
