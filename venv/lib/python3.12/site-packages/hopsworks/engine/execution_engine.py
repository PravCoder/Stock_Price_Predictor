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

from hopsworks.core import dataset_api, execution_api
import os
import logging
import time
import uuid

from hopsworks.client.exceptions import JobExecutionException, RestAPIError


class ExecutionEngine:
    def __init__(self, project_id=None):
        self._dataset_api = dataset_api.DatasetApi(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._log = logging.getLogger(__name__)

    def download_logs(self, execution, path=None):
        """Download execution logs to current directory
        :param execution: execution to download logs for
        :type execution: Execution
        :param path: path to download the logs
        :type path: str
        :return: downloaded stdout and stderr log path
        :rtype: str, str
        :raises: JobExecutionException if path is provided but does not exist
        """
        if path is not None and not os.path.exists(path):
            raise JobExecutionException("Path {} does not exist".format(path))
        elif path is None:
            path = os.getcwd()

        job_logs_dir = "logs-job-{}-exec-{}_{}".format(
            execution.job_name, str(execution.id), str(uuid.uuid4())[:16]
        )
        download_log_dir = os.path.join(path, job_logs_dir)

        if not os.path.exists(download_log_dir):
            os.mkdir(download_log_dir)

        out_path = None
        if execution.stdout_path is not None and self._dataset_api.exists(
            execution.stdout_path
        ):
            out_path = self._download_log(execution.stdout_path, download_log_dir)

        err_path = None
        if execution.stderr_path is not None and self._dataset_api.exists(
            execution.stderr_path
        ):
            err_path = self._download_log(execution.stderr_path, download_log_dir)

        return out_path, err_path

    def _download_log(self, path, download_log_dir):
        max_num_retries = 12
        retries = 0
        download_path = None
        while retries < max_num_retries:
            try:
                download_path = self._dataset_api.download(
                    path, download_log_dir, overwrite=True
                )
                break
            except RestAPIError as e:
                if (
                    e.response.json().get("errorCode", "") == 110021
                    and e.response.status_code == 400
                    and retries < max_num_retries
                ):
                    retries += 1
                    time.sleep(5)
                else:
                    raise e
        return download_path

    def wait_until_finished(self, job, execution):
        """Wait until execution reaches terminal state
        :param execution: job of the execution
        :type execution: Job
        :param execution: execution to monitor
        :type execution: Execution
        :return: The final Execution object
        :rtype: str, str
        """

        is_yarn_job = (
            job.job_type.lower() == "spark"
            or job.job_type.lower() == "pyspark"
            or job.job_type.lower() == "flink"
        )

        updated_execution = self._execution_api._get(job, execution.id)
        execution_state = None
        while updated_execution.success is None:
            updated_execution = self._execution_api._get(job, execution.id)
            if execution_state != updated_execution.state:
                if is_yarn_job:
                    self._log.info(
                        "Waiting for execution to finish. Current state: {}. Final status: {}".format(
                            updated_execution.state, updated_execution.final_status
                        )
                    )
                else:
                    self._log.info(
                        "Waiting for execution to finish. Current state: {}".format(
                            updated_execution.state
                        )
                    )
            execution_state = updated_execution.state
            time.sleep(3)

        # wait for log files to be aggregated, max 6 minutes
        await_time = 120
        log_aggregation_files_exist = False
        self._log.info("Waiting for log aggregation to finish.")
        while not log_aggregation_files_exist and await_time >= 0:
            updated_execution = self._execution_api._get(job, execution.id)

            log_aggregation_files_exist = self._dataset_api.exists(
                updated_execution.stdout_path
            ) and self._dataset_api.exists(updated_execution.stderr_path)

            await_time -= 1
            time.sleep(3)

        time.sleep(5)  # Helps for log aggregation to flush to filesystem

        if is_yarn_job and not updated_execution.success:
            self._log.error(
                "Execution failed with status: {}. See the logs for more information.".format(
                    updated_execution.final_status
                )
            )
        elif not is_yarn_job and not updated_execution.success:
            self._log.error(
                "Execution failed with status: {}. See the logs for more information.".format(
                    updated_execution.state
                )
            )
        else:
            self._log.info("Execution finished successfully.")

        return updated_execution
