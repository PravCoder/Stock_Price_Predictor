#
#   Copyright 2023 Hopsworks AB
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

import os
import json
from hopsworks import client, flink_cluster, util, job
from hopsworks.core import job_api


class FlinkClusterApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name
        self._job_api = job_api.JobsApi(project_id, project_name)

    def get_configuration(self):
        """Get configuration for the Flink cluster.

        # Returns
            `dict`: Default configuration for the Flink cluster,
        # Raises
            `RestAPIError`: If unable to get the job configuration
        """
        return self._job_api.get_configuration("FLINK")

    def setup_cluster(self, name: str, config=None):
        """Create a new flink job representing a flink cluster, or update an existing one.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_config = flink_cluster_api.get_configuration()

        flink_config['appName'] = "myFlinkCluster"

        flink_cluster = flink_cluster_api.setup_cluster(name="myFlinkCluster", config=flink_config)
        ```
        # Arguments
            name: Name of the cluster.
            config: Configuration of the cluster.
        # Returns
            `FlinkCluster`: The FlinkCluster object representing the cluster
        # Raises
            `RestAPIError`: If unable to get the flink cluster object
        """
        if self._job_api.exists(name):
            # If the job already exists, retrieve it
            _flink_cluster = self.get_cluster(name)
            if _flink_cluster._job.job_type != "FLINK":
                raise "This is not a Flink cluster. Please use different name to create new Flink cluster"
            return _flink_cluster
        else:
            # If the job doesn't exists, create a new job
            if config is None:
                config = self.get_configuration()
                config["appName"] = name
            return self._create_cluster(name, config)

    def _create_cluster(self, name: str, config: dict):
        _client = client.get_instance()

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        flink_job = job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )
        flink_cluster_obj = flink_cluster.FlinkCluster(
            flink_job, self._project_id, self._project_name
        )
        print(flink_cluster_obj.get_url())
        return flink_cluster_obj

    def get_cluster(self, name: str):
        """Get the job corresponding to the flink cluster.
        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")
        ```

        # Arguments
            name: Name of the cluster.
        # Returns
            `FlinkCluster`: The FlinkCluster object representing the cluster
        # Raises
            `RestAPIError`: If unable to get the flink cluster object
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        flink_job = job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

        return flink_cluster.FlinkCluster(
            flink_job, self._project_id, self._project_name
        )

    def _get_job(self, execution, job_id):
        """Get specific job from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this execution
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster_api.get_job(execution, job_id)
        ```

        # Arguments
            execution: Execution object.
            job_id: id of the job within this execution
        # Returns
            `Dict`: Dict with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "jobs", job_id]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, headers=headers, with_base_path_params=False
        )

    def _get_jobs(self, execution):
        """Get jobs from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this execution
        flink_cluster_api.get_jobs(execution)
        ```

        # Arguments
            execution: Execution object.
        # Returns
            `List[Dict]`: The array of dicts with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "jobs"]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, headers=headers, with_base_path_params=False
        )["jobs"]

    def _stop_execution(self, execution):
        """Stop specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # stop this execution
        flink_cluster_api.stop_execution(execution)
        ```

        # Arguments
            execution: Execution object.
        # Raises
            `RestAPIError`: If unable to stop the execution
        """
        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "cluster"]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "DELETE", path_params, headers=headers, with_base_path_params=False
        )

    def _stop_job(self, execution, job_id):
        """Stop specific job of the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # stop the job
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster_api.stop_job(execution, job_id)
        ```

        # Arguments
            execution: Execution object.
            job_id: id of the job within this execution
        # Raises
            `RestAPIError`: If unable to stop the job
        """
        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jobs",
            job_id,
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "PATCH", path_params, headers=headers, with_base_path_params=False
        )

    def _get_jars(self, execution):
        """Get already uploaded jars from the specific execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jar files from this execution
        flink_cluster_api.get_jars(execution)
        ```

        # Arguments
            execution: Execution object.
        # Returns
            `List[Dict]`: The array of dicts with jar metadata.
        # Raises
            `RestAPIError`: If unable to get jars from the execution
        """

        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "jars"]
        headers = {"content-type": "application/json"}
        response = _client._send_request(
            "GET", path_params, headers=headers, with_base_path_params=False
        )
        return response["files"]

    def _upload_jar(self, execution, jar_file):
        """Uploaded jar file to the specific execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file jobs from this execution
        jar_file_path = "./flink-example.jar"
        flink_cluster_api.upload_jar(execution, jar_file_path)
        ```

        # Arguments
            execution: Execution object.
            jar_file: path to the jar file.
        # Raises
            `RestAPIError`: If unable to upload jar file
        """

        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jars",
            "upload",
        ]
        files = {
            "jarfile": (
                os.path.basename(jar_file),
                open(jar_file, "rb"),
                "application/x-java-archive",
            )
        }
        _client._send_request(
            "POST", path_params, files=files, with_base_path_params=False
        )
        print("Flink Jar uploaded.")

    def _submit_job(self, execution, jar_id, main_class, job_arguments):
        """Submit job using the specific jar file, already uploaded to this execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file jobs from this execution
        main_class = "com.example.Main"
        job_arguments = "-arg1 arg1 -arg2 arg2"

        #get jar file metadata (and select the 1st one for demo purposes)
        jar_metadata = flink_cluster_api.get_jars(execution)[0]
        jar_id = jar_metadata["id"]
        flink_cluster_api.submit_job(execution, jar_id, main_class, job_arguments=job_arguments)
        ```

        # Arguments
            execution: Execution object.
            jar_id: id if the jar file
            main_class: path to the main class of the the jar file
            job_arguments: Job arguments (if any), defaults to none.
        # Returns
            `str`:  job id.
        # Raises
            `RestAPIError`: If unable to submit the job.
        """

        _client = client.get_instance()
        # Submit execution
        if job_arguments:
            path_params = [
                "hopsworks-api",
                "flinkmaster",
                execution.app_id,
                "jars",
                jar_id,
                f"run?entry-class={main_class}&program-args={job_arguments}",
            ]
        else:
            path_params = [
                "hopsworks-api",
                "flinkmaster",
                execution.app_id,
                "jars",
                jar_id,
                f"run?entry-class{main_class}",
            ]

        headers = {"content-type": "application/json"}
        response = _client._send_request(
            "POST", path_params, headers=headers, with_base_path_params=False
        )

        job_id = response["jobid"]
        print("Submitted Job Id: {}".format(job_id))

        return job_id

    def _job_state(self, execution, job_id):
        """Gets state of the job from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this execution
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster_api.job_status(execution, job_id)
        ```

        # Arguments
            execution: Execution object.
        # Returns
            `str`: status of the job. Possible states:  "INITIALIZING", "CREATED", "RUNNING", "FAILING", "FAILED",
            "CANCELLING", "CANCELED",  "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING".
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """
        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jobs",
            job_id,
        ]
        response = _client._send_request(
            "GET", path_params, with_base_path_params=False
        )

        return response["state"]
