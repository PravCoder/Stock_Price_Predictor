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

from hopsworks.core import git_op_execution_api
from hopsworks.client.exceptions import GitException
import time
import logging


class GitEngine:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._git_op_execution_api = git_op_execution_api.GitOpExecutionApi(
            project_id, project_name
        )
        self._log = logging.getLogger(__name__)

    def execute_op_blocking(self, git_op, command):
        """Poll a git execution status until it reaches a terminal state
        :param git_op: git execution to monitor
        :type git_op: GitOpExecution
        :param command: git operation running
        :type command: str
        :return: The final GitOpExecution object
        :rtype: GitOpExecution
        """

        while git_op.success is None:
            self._log.info(
                "Running command {}, current status {}".format(command, git_op.state)
            )
            git_op = self._git_op_execution_api._get_execution(
                git_op.repository.id, git_op.id
            )
            time.sleep(5)

        if git_op.success is False:
            raise GitException(
                "Git command failed with the message: {}".format(
                    git_op.command_result_message
                )
            )
        else:
            self._log.info("Git command {} finished".format(command))

        return git_op
