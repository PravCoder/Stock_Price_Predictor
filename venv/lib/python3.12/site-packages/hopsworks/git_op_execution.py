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
from hopsworks import constants, git_repo


class GitOpExecution:
    def __init__(
        self,
        id,
        submission_time,
        execution_start,
        execution_stop,
        user,
        git_command_configuration,
        state,
        config_secret,
        repository,
        command_result_message=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        project_id=None,
        project_name=None,
        **kwargs,
    ):
        self._id = id
        self._submission_time = submission_time
        self._execution_start = execution_start
        self._execution_stop = execution_stop
        self._user = user
        self._git_command_configuration = git_command_configuration
        self._state = state
        self._command_result_message = command_result_message
        self._repository = git_repo.GitRepo.from_response_json(
            repository, project_id, project_name
        )

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized, project_id=project_id, project_name=project_name)

    @property
    def id(self):
        """Id of the execution"""
        return self._id

    @property
    def submission_time(self):
        """Timestamp when the execution was submitted"""
        return self._submission_time

    @property
    def execution_start(self):
        """Timestamp when the execution started"""
        return self._execution_start

    @property
    def execution_stop(self):
        """Timestamp when the execution stopped"""
        return self._execution_stop

    @property
    def user(self):
        """User that issued the execution"""
        return self._user

    @property
    def git_command_configuration(self):
        """Configuration for the git command"""
        return self._git_command_configuration

    @property
    def state(self):
        """State of the git execution"""
        return self._state

    @property
    def command_result_message(self):
        """Results message from the execution"""
        return self._command_result_message

    @property
    def repository(self):
        """Git repository of the execution"""
        return self._repository

    @property
    def success(self):
        """Boolean to indicate if execution ran successfully or failed

        ```
        # Returns
            `bool`. True if execution ran successfully. False if execution failed.
        """
        if self.state is not None and self.state.upper() in constants.GIT.ERROR_STATES:
            return False
        elif (
            self.state is not None
            and self.state.upper() in constants.GIT.SUCCESS_STATES
        ):
            return True
        else:
            return None
