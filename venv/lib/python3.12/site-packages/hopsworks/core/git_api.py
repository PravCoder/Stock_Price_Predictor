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

from hopsworks import (
    client,
    git_repo,
    git_op_execution,
    util,
    git_commit,
    git_file_status,
)
from hopsworks.client.exceptions import GitException
from hopsworks.engine import git_engine
from hopsworks.core import git_provider_api
from typing import List, Union
from hopsworks.git_file_status import GitFileStatus

import json
import logging


class GitApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name
        self._git_engine = git_engine.GitEngine(project_id, project_name)
        self._git_provider_api = git_provider_api.GitProviderApi(
            project_id, project_name
        )
        self._log = logging.getLogger(__name__)

    def clone(self, url: str, path: str, provider: str = None, branch: str = None):
        """Clone a new Git Repo in to Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        git_api = project.get_git_api()

        git_repo = git_api.clone("https://github.com/logicalclocks/hops-examples.git", "Resources", "GitHub")

        ```
        # Arguments
            url: Url to the git repository
            path: Path in Hopsworks Filesystem to clone the repo to
            provider: The git provider where the repo is currently hosted. Valid values are "GitHub", "GitLab" and "BitBucket".
            branch: Optional branch to clone, defaults to configured main branch
        # Returns
            `GitRepo`: Git repository object
        # Raises
            `RestAPIError`: If unable to clone the git repository.
        """

        _client = client.get_instance()

        # Support absolute and relative path to dataset
        path = util.convert_to_abs(path, self._project_name)

        if provider is None:
            provider = self._git_provider_api._get_default_configured_provider()

        path_params = ["project", self._project_id, "git", "clone"]

        clone_config = {
            "url": url,
            "path": path,
            "provider": provider,
            "branch": branch,
        }
        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps(clone_config),
                query_params=query_params,
            ),
            self._project_id,
            self._project_name,
        )
        print(
            "Git clone operation running, explore it at "
            + util.get_hostname_replaced_url(
                "/p/" + str(self._project_id) + "/settings/git"
            )
        )
        git_op = self._git_engine.execute_op_blocking(git_op, "CLONE")
        created_repo = self.get_repo(git_op.repository.name, git_op.repository.path)
        return created_repo

    def get_repos(self):
        """Get the existing Git repositories

        # Returns
            `List[GitRepo]`: List of git repository objects
        # Raises
            `RestAPIError`: If unable to get the repositories
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}
        return git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

    def get_providers(self):
        """Get the configured Git providers

        # Returns
            `List[GitProvider]`: List of git provider objects
        # Raises
            `RestAPIError`: If unable to get the git providers
        """
        return self._git_provider_api._get_providers()

    def get_provider(self, provider: str):
        """Get the configured Git provider

        # Arguments
            provider: Name of git provider. Valid values are "GitHub", "GitLab" and "BitBucket".
        # Returns
            `GitProvider`: The git provider
        # Raises
            `RestAPIError`: If unable to get the git provider
        """
        return self._git_provider_api._get_provider(provider)

    def set_provider(self, provider: str, username: str, token: str):
        """Configure a Git provider

        ```python

        import hopsworks

        project = hopsworks.login()

        git_api = project.get_git_api()

        git_api.set_provider("GitHub", "my_user", "my_token")

        ```
        # Arguments
            provider: Name of git provider. Valid values are "GitHub", "GitLab" and "BitBucket".
            username: Username for the git provider service
            token: Token to set for the git provider service
        # Raises
            `RestAPIError`: If unable to configure the git provider
        """
        self._git_provider_api._set_provider(provider, username, token)

    def get_repo(self, name: str, path: str = None):
        """Get the cloned Git repository

        # Arguments
            name: Name of git repository
            path: Optional path to specify if multiple git repos with the same name exists in the project
        # Returns
            `GitRepo`: The git repository
        # Raises
            `RestAPIError`: If unable to get the git repository
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}

        repos = git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

        if path is not None:
            path = util.convert_to_abs(path, self._project_name)

        filtered_repos = []
        for repository in repos:
            if repository.name == name:
                if path is None:
                    filtered_repos.append(repository)
                elif repository.path == path:
                    filtered_repos.append(repository)

        if len(filtered_repos) == 1:
            return filtered_repos[0]
        elif len(filtered_repos) > 1:
            raise GitException(
                "Multiple repositories found matching name {}. Please specify the repository by setting the path keyword, for example path='Resources/{}'.".format(
                    name, name
                )
            )
        else:
            raise GitException("No git repository found matching name {}".format(name))

    def _delete_repo(self, repo_id):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]
        _client._send_request("DELETE", path_params)

    def _create(self, repo_id, branch: str, checkout=False):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "expand": "repository"}

        if checkout:
            query_params["action"] = "CREATE_CHECKOUT"
        else:
            query_params["action"] = "CREATE"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _delete(self, repo_id, branch: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {
            "action": "DELETE",
            "branchName": branch,
            "expand": "repository",
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _checkout(
        self, repo_id, branch: str = None, commit: str = None, force: bool = False
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "commit": commit, "expand": "repository"}

        if force:
            query_params["action"] = "CHECKOUT_FORCE"
        else:
            query_params["action"] = "CHECKOUT"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _status(self, repo_id):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "STATUS", "expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
            ),
            self._project_id,
            self._project_name,
        )
        git_op = self._git_engine.execute_op_blocking(git_op, query_params["action"])

        status_dict = json.loads(git_op.command_result_message)
        file_status = None
        if status_dict is not None and type(status_dict["status"]) is list:
            file_status = []
            for status in status_dict["status"]:
                file_status.append(
                    git_file_status.GitFileStatus.from_response_json(status)
                )
        else:
            self._log.info("Nothing to commit, working tree clean")

        return file_status

    def _commit(self, repo_id, message: str, all=False, files=None):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "COMMIT", "expand": ["repository", "user"]}
        commit_config = {
            "type": "commitCommandConfiguration",
            "message": message,
            "all": all,
            "files": files,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(commit_config),
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _push(self, repo_id, branch: str, force: bool = False, remote: str = None):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PUSH", "expand": ["repository", "user"]}
        push_config = {
            "type": "pushCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _pull(self, repo_id, branch: str, force: bool = False, remote: str = None):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PULL", "expand": ["repository", "user"]}
        push_config = {
            "type": "pullCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _checkout_files(self, repo_id, files: Union[List[str], List[GitFileStatus]]):
        files = util.convert_git_status_to_files(files)

        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "file",
        ]

        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps({"files": files}),
            ),
            self._project_id,
            self._project_name,
        )
        _ = self._git_engine.execute_op_blocking(git_op, "CHECKOUT_FILES")

    def _get_commits(self, repo_id, branch: str):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
            branch,
            "commit",
        ]

        headers = {"content-type": "application/json"}
        return git_commit.GitCommit.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )
