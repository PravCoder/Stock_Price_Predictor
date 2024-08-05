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
from hopsworks.core import git_remote_api
from hopsworks import util


class GitRemote:
    def __init__(
        self,
        remote_name=None,
        remote_url=None,
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
        self._name = remote_name
        self._url = remote_url

        self._git_remote_api = git_remote_api.GitRemoteApi(project_id, project_name)

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**remote, project_id=project_id)
                for remote in json_decamelized["items"]
            ]
        else:
            return cls(
                **json_decamelized, project_id=project_id, project_name=project_name
            )

    @property
    def name(self):
        """Name of the remote"""
        return self._name

    @property
    def url(self):
        """Url of the remote"""
        return self._url

    def delete(self):
        """Remove the git remote from the repo.

        # Raises
            `RestAPIError`.
        """
        self._git_remote_api._delete(self._repo_id, self.name)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitRemote({self._name!r}, {self._url!r})"
