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

import time
from hopsworks.engine import execution_engine
from hopsworks.core import execution_api
from hopsworks.core import flink_cluster_api
from hopsworks import util


class FlinkCluster:
    def __init__(
        self,
        job,
        project_id=None,
        project_name=None,
        **kwargs,
    ):
        self._job = job
        self._project_id = project_id
        self._project_name = project_name
        self._execution_id = None

        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._flink_cluster_api = flink_cluster_api.FlinkClusterApi(
            project_id, project_name
        )

    def _get_execution(self, assert_running=False):
        current_execution = None
        if self._job is not None:
            executions = self._job.get_executions()
            for exec in executions:
                # If previous start was used on the cluster, always refer to the specific execution_id
                if self._execution_id is not None:
                    if exec.id == self._execution_id:
                        current_execution = exec
                        break
                # Otherwise get the first running one and set the id
                elif exec.success is None:
                    self._execution_id = exec.id
                    current_execution = exec
                    break
        if assert_running is True and current_execution.success is not None:
            raise Exception(
                "This operation requires the FlinkCluster to be running. To start it invoke .start()"
            )
        return current_execution

    def _count_ongoing_executions(self):
        ongoing_executions = 0
        if self._job is not None:
            executions = self._job.get_executions()
            for exec in executions:
                if exec.success is None:
                    ongoing_executions += 1
        return ongoing_executions

    def start(self, await_time=1800):
        """Start the flink cluster and wait until it reaches RUNNING state.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.start()
        ```
        # Arguments
            await_time: defaults to 1800 seconds to account for auto-scale mechanisms.
        # Raises
            `RestAPIError`: If unable to start the flink cluster.
        """

        if self._count_ongoing_executions() > 0:
            raise Exception(
                "There is already a running FlinkCluster. Use FlinkClusterApi.get_cluster('{}') to get a reference to it.".format(
                    self._job.name
                )
            )

        execution = self._execution_api._start(self._job)
        updated_execution = self._execution_api._get(self._job, execution.id)
        while updated_execution.state == "INITIALIZING":
            updated_execution = self._execution_api._get(self._job, execution.id)
            if updated_execution.state == "RUNNING":
                print("Cluster is running")
                break

            self._execution_engine._log.info(
                "Waiting for cluster to start. Current state: {}.".format(
                    updated_execution.state
                )
            )

            await_time -= 1
            time.sleep(3)

        if updated_execution.state != "RUNNING":
            raise Exception(
                "FlinkCluster {} did not start within the allocated time and exited with state {}".format(
                    execution.id, execution.state
                )
            )

        self._execution_id = execution.id
        return self

    def get_jobs(self):
        """Get jobs from the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this flink cluster
        flink_cluster.get_jobs()
        ```

        # Returns
            `List[Dict]`: The array of dicts with flink job id and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the cluster
        """

        return self._flink_cluster_api._get_jobs(
            self._get_execution(assert_running=True)
        )

    def get_job(self, job_id):
        """Get specific job from the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this cluster
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.get_job(job_id)
        ```

        # Arguments
            job_id: id of the job within this cluster
        # Returns
            `Dict`: Dict with flink job id and status of the job.
        # Raises
            `RestAPIError`: If unable to get the job from the cluster
        """

        return self._flink_cluster_api._get_job(
            self._get_execution(assert_running=True), job_id
        )

    def stop_job(self, job_id):
        """Stop specific job in the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # stop the job
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.stop_job(job_id)
        ```

        # Arguments
            job_id: id of the job within this flink cluster.
        # Raises
            `RestAPIError`: If unable to stop the job
        """
        self._flink_cluster_api._stop_job(
            self._get_execution(assert_running=True), job_id
        )

    def get_jars(self):
        """Get already uploaded jars from the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jar files from this cluster
        flink_cluster.get_jars()
        ```

        # Returns
            `List[Dict]`: The array of dicts with jar metadata.
        # Raises
            `RestAPIError`: If unable to get jars from the flink cluster.
        """
        return self._flink_cluster_api._get_jars(
            self._get_execution(assert_running=True)
        )

    def upload_jar(self, jar_file):
        """Upload jar file to the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file to this cluster
        jar_file_path = "./flink-example.jar"
        flink_cluster.upload_jar(jar_file_path)
        ```

        # Arguments
            jar_file: path to the jar file.
        # Raises
            `RestAPIError`: If unable to upload jar file
        """

        self._flink_cluster_api._upload_jar(
            self._get_execution(assert_running=True), jar_file
        )

    def submit_job(self, jar_id, main_class, job_arguments=None):
        """Submit job using the specific jar file uploaded to the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # upload jar file to this cluster
        main_class = "com.example.Main"
        job_arguments = "-arg1 arg1 -arg2 arg2"
        jar_file_path = "./flink-example.jar"
        flink_cluster.upload_jar(jar_file_path)

        #get jar file metadata (and select the 1st one for demo purposes)
        jar_metadata = flink_cluster.get_jars()[0]
        jar_id = jar_metadata["id"]
        flink_cluster.submit_job(jar_id, main_class, job_arguments=job_arguments)
        ```

        # Arguments
            jar_id: id if the jar file
            main_class: path to the main class of the jar file
            job_arguments: Job arguments (if any), defaults to none.
        # Returns
            `str`:  job id.
        # Raises
            `RestAPIError`: If unable to submit the job.
        """

        return self._flink_cluster_api._submit_job(
            self._get_execution(assert_running=True), jar_id, main_class, job_arguments
        )

    def job_state(self, job_id):
        """Gets state of the job submitted to the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get jobs from this flink cluster
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        flink_cluster.job_state(job_id)
        ```

        # Arguments
            job_id: id of the job within this flink cluster
        # Returns
            `str`: status of the job. Possible states:  "INITIALIZING", "CREATED", "RUNNING", "FAILING", "FAILED",
            "CANCELLING", "CANCELED",  "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING".
        # Raises
            `RestAPIError`: If unable to get the job state from the flink cluster.
        """

        return self._flink_cluster_api._job_state(
            self._get_execution(assert_running=True), job_id
        )

    def stop(self):
        """Stop this cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.stop()
        ```

        # Raises
            `RestAPIError`: If unable to stop the flink cluster.
        """
        exec = self._get_execution(assert_running=False)
        if exec is None or exec.success is not None:
            return
        else:
            self._flink_cluster_api._stop_execution(
                self._get_execution(assert_running=False)
            )

    @property
    def id(self):
        """Id of the cluster"""
        return self._job._id

    @property
    def name(self):
        """Name of the cluster"""
        return self._job._name

    @property
    def creation_time(self):
        """Date of creation for the cluster"""
        return self._job._creation_time

    @property
    def config(self):
        """Configuration for the cluster"""
        return self._job._config

    @property
    def creator(self):
        """Creator of the cluster"""
        return self._job._creator

    @property
    def state(self):
        """State of the cluster"""
        current_exec = self._get_execution(assert_running=False)
        if current_exec is not None:
            return current_exec.state
        else:
            return None

    def get_url(self):
        path = "/p/" + str(self._project_id) + "/jobs/named/" + self.name
        return (
            "FlinkCluster created successfully, explore it at "
            + util.get_hostname_replaced_url(path)
        )
