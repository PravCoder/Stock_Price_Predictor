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

import json

from hopsworks import client, job, util, job_schedule
from hopsworks.client.exceptions import RestAPIError


class JobsApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

    def create_job(self, name: str, config: dict):
        """Create a new job or update an existing one.

        ```python

        import hopsworks

        project = hopsworks.login()

        jobs_api = project.get_jobs_api()

        spark_config = jobs_api.get_configuration("PYSPARK")

        spark_config['appPath'] = "/Resources/my_app.py"

        job = jobs_api.create_job("my_spark_job", spark_config)

        ```
        # Arguments
            name: Name of the job.
            config: Configuration of the job.
        # Returns
            `Job`: The Job object
        # Raises
            `RestAPIError`: If unable to create the job
        """
        _client = client.get_instance()

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        created_job = job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )
        print(created_job.get_url())
        return created_job

    def get_job(self, name: str):
        """Get a job.

        # Arguments
            name: Name of the job.
        # Returns
            `Job`: The Job object
        # Raises
            `RestAPIError`: If unable to get the job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        return job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

    def get_jobs(self):
        """Get all jobs.

        # Returns
            `List[Job]`: List of Job objects
        # Raises
            `RestAPIError`: If unable to get the jobs
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
        ]
        query_params = {"expand": ["creator"]}
        return job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

    def exists(self, name: str):
        """Check if a job exists.

        # Arguments
            name: Name of the job.
        # Returns
            `bool`: True if the job exists, otherwise False
        # Raises
            `RestAPIError`: If unable to check the existence of the job
        """
        try:
            self.get_job(name)
            return True
        except RestAPIError:
            return False

    def get_configuration(self, type: str):
        """Get configuration for the specific job type.

        # Arguments
            type: Type of the job. Currently, supported types include: SPARK, PYSPARK, PYTHON, DOCKER, FLINK.
        # Returns
            `dict`: Default job configuration
        # Raises
            `RestAPIError`: If unable to get the job configuration
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            type.lower(),
            "configuration",
        ]

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def _delete(self, job):
        """Delete the job and all executions.
        :param job: metadata object of job to delete
        :type job: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            str(job.name),
        ]
        _client._send_request("DELETE", path_params)

    def _update_job(self, name: str, config: dict):
        """Update the job.
        :param name: name of the job
        :type name: str
        :param config: new job configuration
        :type config: dict
        :return: The updated Job object
        :rtype: Job
        """
        _client = client.get_instance()

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )

    def _schedule_job(self, name, schedule_config):
        _client = client.get_instance()
        path_params = ["project", self._project_id, "jobs", name, "schedule", "v2"]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config["id"] else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    def _delete_schedule_job(self, name):
        _client = client.get_instance()
        path_params = ["project", self._project_id, "jobs", name, "schedule", "v2"]

        return _client._send_request(
            "DELETE",
            path_params,
        )
