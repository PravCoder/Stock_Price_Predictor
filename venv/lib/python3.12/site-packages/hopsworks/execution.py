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
from hopsworks.engine import execution_engine
from hopsworks.core import execution_api
from hopsworks import constants, util


class Execution:
    def __init__(
        self,
        id=None,
        state=None,
        final_status=None,
        submission_time=None,
        stdout_path=None,
        stderr_path=None,
        app_id=None,
        hdfs_user=None,
        args=None,
        progress=None,
        user=None,
        files_to_remove=None,
        duration=None,
        flink_master_url=None,
        monitoring=None,
        type=None,
        href=None,
        project_id=None,
        job=None,
        **kwargs,
    ):
        self._id = id
        self._final_status = final_status
        self._state = state
        self._submission_time = submission_time
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path
        self._args = args
        self._progress = progress
        self._user = user
        self._duration = duration
        self._monitoring = monitoring
        self._app_id = app_id
        self._hdfs_user = hdfs_user
        self._job = job
        self._project_id = project_id

        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)

    @classmethod
    def from_response_json(cls, json_dict, project_id, job):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized:
            return cls(**json_decamelized, project_id=project_id, job=job)
        elif json_decamelized["count"] == 0:
            return []
        else:
            return [
                cls(**execution, project_id=project_id, job=job)
                for execution in json_decamelized["items"]
            ]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    @property
    def id(self):
        """Id of the execution"""
        return self._id

    @property
    def job_name(self):
        """Name of the job the execution belongs to"""
        return self._job.name

    @property
    def job_type(self):
        """Type of the job the execution belongs to"""
        return self._job.job_type

    @property
    def state(self):
        """Current state of the execution."""
        return self._state

    @property
    def final_status(self):
        """Final status of the execution. Can be UNDEFINED, SUCCEEDED, FAILED or KILLED."""
        return self._final_status

    @property
    def submission_time(self):
        """Timestamp when the execution was submitted"""
        return self._submission_time

    @property
    def stdout_path(self):
        """Path in Hopsworks Filesystem to stdout log file"""
        return self._stdout_path

    @property
    def stderr_path(self):
        """Path in Hopsworks Filesystem to stderr log file"""
        return self._stderr_path

    @property
    def app_id(self):
        """Application id for the execution"""
        return self._app_id

    @property
    def hdfs_user(self):
        """Filesystem user for the execution."""
        return self._hdfs_user

    @property
    def args(self):
        """Arguments set for the execution."""
        return self._args

    @property
    def progress(self):
        """Progress of the execution."""
        return self._progress

    @property
    def user(self):
        """User that submitted the execution."""
        return self._user

    @property
    def duration(self):
        """Duration in milliseconds the execution ran."""
        return self._duration

    @property
    def success(self):
        """Boolean to indicate if execution ran successfully or failed

        # Returns
            `bool`. True if execution ran successfully. False if execution failed or was killed.
        """

        is_yarn_job = (
            self.job_type.lower() == "spark"
            or self.job_type.lower() == "pyspark"
            or self.job_type.lower() == "flink"
        )

        if is_yarn_job:
            if self.final_status in constants.JOBS.ERROR_STATES:
                return False
            elif self.final_status in constants.JOBS.SUCCESS_STATES:
                return True
        else:
            if self.state in constants.JOBS.ERROR_STATES:
                return False
            elif self.state in constants.JOBS.SUCCESS_STATES:
                return True
        return None

    def download_logs(self, path=None):
        """Download stdout and stderr logs for the execution
        Example for downloading and printing the logs

        ```python

        # Download logs
        out_log_path, err_log_path = execution.download_logs()

        out_fd = open(out_log_path, "r")
        print(out_fd.read())

        err_fd = open(err_log_path, "r")
        print(err_fd.read())

        ```

        # Arguments
            path: path to download the logs. must be `str`
        # Returns
            `str`. Path to downloaded log for stdout.
            `str`. Path to downloaded log for stderr.
        """
        return self._execution_engine.download_logs(self, path)

    def delete(self):
        """Delete the execution
        !!! danger "Potentially dangerous operation"
            This operation deletes the execution.
        # Raises
            `RestAPIError`.
        """
        self._execution_api._delete(self._job.name, self.id)

    def await_termination(self):
        """Wait until execution reaches terminal state
        # Raises
            `RestAPIError`.
        """
        self._execution_engine.wait_until_finished(self._job, self)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Execution({self._final_status!r}, {self._state!r}, {self._submission_time!r}, {self._args!r})"

    def get_url(self):
        path = (
            "/p/"
            + str(self._project_id)
            + "/jobs/named/"
            + self.job_name
            + "/executions"
        )
        return "Execution started, explore it at " + util.get_hostname_replaced_url(
            path
        )
