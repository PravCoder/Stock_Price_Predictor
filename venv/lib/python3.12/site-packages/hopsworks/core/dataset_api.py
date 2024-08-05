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

import math
import os
import time
from tqdm.auto import tqdm
import shutil
import logging
import copy

from hopsworks import client
from hopsworks.client.exceptions import RestAPIError
from hopsworks.client.exceptions import DatasetException
from concurrent.futures import ThreadPoolExecutor, wait


class Chunk:
    def __init__(self, content, number, status):
        self.content = content
        self.number = number
        self.status = status
        self.retries = 0


class DatasetApi:
    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id
        self._log = logging.getLogger(__name__)

    DEFAULT_FLOW_CHUNK_SIZE = 1048576
    FLOW_PERMANENT_ERRORS = [404, 413, 415, 500, 501]

    def download(self, path: str, local_path: str = None, overwrite: bool = False):
        """Download file from Hopsworks Filesystem to the current working directory.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        downloaded_file_path = dataset_api.download("Resources/my_local_file.txt")

        ```
        # Arguments
            path: path in Hopsworks filesystem to the file
            local_path: path where to download the file in the local filesystem
            overwrite: overwrite local file if exists
        # Returns
            `str`: Path to downloaded file
        # Raises
            `RestAPIError`: If unable to download the file
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "dataset",
            "download",
            "with_auth",
            path,
        ]
        query_params = {"type": "DATASET"}

        # Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
        # Download in CWD if local_path not specified
        if local_path is None:
            local_path = os.path.join(os.getcwd(), os.path.basename(path))
        # If local_path specified, ensure it is absolute
        else:
            if os.path.isabs(local_path):
                local_path = os.path.join(local_path, os.path.basename(path))
            else:
                local_path = os.path.join(
                    os.getcwd(), local_path, os.path.basename(path)
                )

        if os.path.exists(local_path):
            if overwrite:
                if os.path.isfile:
                    os.remove(local_path)
                else:
                    shutil.rmtree(local_path)
            else:
                raise IOError(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        file_size = int(self._get(path)["attributes"]["size"])
        with _client._send_request(
            "GET", path_params, query_params=query_params, stream=True
        ) as response:
            with open(local_path, "wb") as f:
                pbar = None
                try:
                    pbar = tqdm(
                        total=file_size,
                        bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                        desc="Downloading",
                    )
                except Exception:
                    self._log.exception("Failed to initialize progress bar.")
                    self._log.info("Starting download")

                for chunk in response.iter_content(
                    chunk_size=self.DEFAULT_FLOW_CHUNK_SIZE
                ):
                    f.write(chunk)

                    if pbar is not None:
                        pbar.update(len(chunk))

                if pbar is not None:
                    pbar.close()
                else:
                    self._log.info("Download finished")

        return local_path

    def upload(
        self,
        local_path: str,
        upload_path: str,
        overwrite: bool = False,
        chunk_size=1048576,
        simultaneous_uploads=3,
        max_chunk_retries=1,
        chunk_retry_interval=1,
    ):
        """Upload a file to the Hopsworks filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        uploaded_file_path = dataset_api.upload("my_local_file.txt", "Resources")

        ```
        # Arguments
            local_path: local path to file to upload
            upload_path: path to directory where to upload the file in Hopsworks Filesystem
            overwrite: overwrite file if exists
            chunk_size: upload chunk size in bytes. Default 1048576 bytes
            simultaneous_uploads: number of simultaneous chunks to upload. Default 3
            max_chunk_retries: maximum retry for a chunk. Default is 1
            chunk_retry_interval: chunk retry interval in seconds. Default is 1sec
        # Returns
            `str`: Path to uploaded file
        # Raises
            `RestAPIError`: If unable to upload the file
        """
        # local path could be absolute or relative,
        if not os.path.isabs(local_path) and os.path.exists(
            os.path.join(os.getcwd(), local_path)
        ):
            local_path = os.path.join(os.getcwd(), local_path)

        file_size = os.path.getsize(local_path)

        _, file_name = os.path.split(local_path)

        destination_path = upload_path + "/" + file_name

        if self.exists(destination_path):
            if overwrite:
                self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        num_chunks = math.ceil(file_size / chunk_size)

        base_params = self._get_flow_base_params(
            file_name, num_chunks, file_size, chunk_size
        )

        chunk_number = 1
        with open(local_path, "rb") as f:
            pbar = None
            try:
                pbar = tqdm(
                    total=file_size,
                    bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                    desc="Uploading",
                )
            except Exception:
                self._log.exception("Failed to initialize progress bar.")
                self._log.info("Starting upload")
            with ThreadPoolExecutor(simultaneous_uploads) as executor:
                while True:
                    chunks = []
                    for _ in range(simultaneous_uploads):
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        chunks.append(Chunk(chunk, chunk_number, "pending"))
                        chunk_number += 1

                    if len(chunks) == 0:
                        break

                    # upload each chunk and update pbar
                    futures = [
                        executor.submit(
                            self._upload_chunk,
                            base_params,
                            upload_path,
                            file_name,
                            chunk,
                            pbar,
                            max_chunk_retries,
                            chunk_retry_interval,
                        )
                        for chunk in chunks
                    ]
                    # wait for all upload tasks to complete
                    _, _ = wait(futures)
                    try:
                        _ = [future.result() for future in futures]
                    except Exception as e:
                        if pbar is not None:
                            pbar.close()
                        raise e

            if pbar is not None:
                pbar.close()
            else:
                self._log.info("Upload finished")

        return upload_path + "/" + os.path.basename(local_path)

    def _upload_chunk(
        self,
        base_params,
        upload_path,
        file_name,
        chunk: Chunk,
        pbar,
        max_chunk_retries,
        chunk_retry_interval,
    ):
        query_params = copy.copy(base_params)
        query_params["flowCurrentChunkSize"] = len(chunk.content)
        query_params["flowChunkNumber"] = chunk.number

        chunk.status = "uploading"
        while True:
            try:
                self._upload_request(
                    query_params, upload_path, file_name, chunk.content
                )
                break
            except RestAPIError as re:
                chunk.retries += 1
                if (
                    re.response.status_code in DatasetApi.FLOW_PERMANENT_ERRORS
                    or chunk.retries > max_chunk_retries
                ):
                    chunk.status = "failed"
                    raise re
                time.sleep(chunk_retry_interval)
                continue

        chunk.status = "uploaded"

        if pbar is not None:
            pbar.update(query_params["flowCurrentChunkSize"])

    def _get_flow_base_params(self, file_name, num_chunks, size, chunk_size):
        return {
            "templateId": -1,
            "flowChunkSize": chunk_size,
            "flowTotalSize": size,
            "flowIdentifier": str(size) + "_" + file_name,
            "flowFilename": file_name,
            "flowRelativePath": file_name,
            "flowTotalChunks": num_chunks,
        }

    def _upload_request(self, params, path, file_name, chunk):
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", "upload", path]

        # Flow configuration params are sent as form data
        _client._send_request(
            "POST", path_params, data=params, files={"file": (file_name, chunk)}
        )

    def _get(self, path: str):
        """Get dataset.

        :param path: path to check
        :type path: str
        :return: dataset metadata
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def exists(self, path: str):
        """Check if a file exists in the Hopsworks Filesystem.

        # Arguments
            path: path to check
        # Returns
            `bool`: True if exists, otherwise False
        # Raises
            `RestAPIError`: If unable to check existence for the path
        """
        try:
            self._get(path)
            return True
        except RestAPIError:
            return False

    def remove(self, path: str):
        """Remove a path in the Hopsworks Filesystem.

        # Arguments
            path: path to remove
        # Raises
            `RestAPIError`: If unable to remove the path
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        _client._send_request("DELETE", path_params)

    def mkdir(self, path: str):
        """Create a directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.mkdir("Resources/my_dir")

        ```
        # Arguments
            path: path to directory
        # Returns
            `str`: Path to created directory
        # Raises
            `RestAPIError`: If unable to create the directory
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        query_params = {
            "action": "create",
            "searchable": "true",
            "generate_readme": "false",
            "type": "DATASET",
        }
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )["attributes"]["path"]

    def copy(self, source_path: str, destination_path: str, overwrite: bool = False):
        """Copy a file or directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.copy("Resources/myfile.txt", "Logs/myfile.txt")

        ```
        # Arguments
            source_path: the source path to copy
            destination_path: the destination path
            overwrite: overwrite destination if exists
        # Raises
            `RestAPIError`: If unable to perform the copy
        """
        if self.exists(destination_path):
            if overwrite:
                self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        destination_path
                    )
                )

        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", source_path]
        query_params = {
            "action": "copy",
            "destination_path": destination_path,
        }
        _client._send_request("POST", path_params, query_params=query_params)

    def move(self, source_path: str, destination_path: str, overwrite: bool = False):
        """Move a file or directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.move("Resources/myfile.txt", "Logs/myfile.txt")

        ```
        # Arguments
            source_path: the source path to move
            destination_path: the destination path
            overwrite: overwrite destination if exists
        # Raises
            `RestAPIError`: If unable to perform the move
        """

        if self.exists(destination_path):
            if overwrite:
                self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        destination_path
                    )
                )

        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", source_path]
        query_params = {
            "action": "move",
            "destination_path": destination_path,
        }
        _client._send_request("POST", path_params, query_params=query_params)
