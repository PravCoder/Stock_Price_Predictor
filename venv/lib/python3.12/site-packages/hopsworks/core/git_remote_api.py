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

from hopsworks import client, git_op_execution, git_remote
from hopsworks.engine import git_engine


class GitRemoteApi:
    def __init__(self, project_id, project_name):
        self._git_engine = git_engine.GitEngine(project_id, project_name)
        self._project_id = project_id
        self._project_name = project_name

    def _get(self, repo_id, name: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "remote",
            str(name),
        ]

        remote = git_remote.GitRemote.from_response_json(
            _client._send_request("GET", path_params),
            self._project_id,
            self._project_name,
        )
        remote._repo_id = repo_id
        return remote

    def _get_remotes(self, repo_id):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "remote",
        ]

        remotes = git_remote.GitRemote.from_response_json(
            _client._send_request("GET", path_params),
            self._project_id,
            self._project_name,
        )
        for remote in remotes:
            remote._repo_id = repo_id
        return remotes

    def _add(self, repo_id, name: str, url: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "remote",
        ]

        query_params = {
            "action": "ADD",
            "url": url,
            "name": name,
            "expand": ["repository", "user"],
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, "ADD_REMOTE")
        return self._get(repo_id, name)

    def _delete(self, repo_id, name: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "remote",
        ]

        query_params = {
            "action": "DELETE",
            "name": name,
            "expand": ["repository", "user"],
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, "DELETE_REMOTE")
