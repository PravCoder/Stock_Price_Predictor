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

import humps
import json
from hopsworks.core import git_provider_api
from hopsworks import util


class GitProvider:
    def __init__(
        self,
        username=None,
        token=None,
        git_provider=None,
        url=None,
        name=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        project_id=None,
        project_name=None,
        **kwargs,
    ):
        self._username = username
        self._git_provider = git_provider

        self._git_provider_api = git_provider_api.GitProviderApi(
            project_id, project_name
        )

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        # Count is not set by the backend so parse based on items array
        json_decamelized = humps.decamelize(json_dict)
        if len(json_decamelized["items"]) == 0:
            return []
        else:
            return [
                cls(**provider, project_id=project_id, project_name=project_name)
                for provider in json_decamelized["items"]
            ]

    @property
    def username(self):
        """Username set for the provider"""
        return self._username

    @property
    def git_provider(self):
        """Name of the provider, can be GitHub, GitLab or BitBucket"""
        return self._git_provider

    def delete(self):
        """Remove the git provider configuration.

        # Raises
            `RestAPIError`.
        """
        self._git_provider_api._delete_provider(self.git_provider)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitProvider({self._username!r}, {self._git_provider!r})"
