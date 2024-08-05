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

from hopsworks import client, execution


class ExecutionsApi:
    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id

    def _start(self, job, args: str = None):
        _client = client.get_instance()
        path_params = ["project", self._project_id, "jobs", job.name, "executions"]

        return execution.Execution.from_response_json(
            _client._send_request("POST", path_params, data=args), self._project_id, job
        )

    def _get(self, job, id):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            job.name,
            "executions",
            id,
        ]

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request("GET", path_params, headers=headers),
            self._project_id,
            job,
        )

    def _get_all(self, job):
        _client = client.get_instance()
        path_params = ["project", self._project_id, "jobs", job.name, "executions"]

        query_params = {"sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            self._project_id,
            job,
        )

    def _delete(self, job_name, id):
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            job_name,
            "executions",
            id,
        ]
        _client._send_request("DELETE", path_params)
