#
#   Copyright 2021 Logical Clocks AB
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
import json
from hsml.client.exceptions import RestAPIError
import time
import copy
from tqdm.auto import tqdm

from hsml import client, tag
from concurrent.futures import ThreadPoolExecutor, wait


class Chunk:
    def __init__(self, content, number, status):
        self.content = content
        self.number = number
        self.status = status
        self.retries = 0


class DatasetApi:
    def __init__(self):
        pass

    DEFAULT_UPLOAD_FLOW_CHUNK_SIZE = 10
    DEFAULT_UPLOAD_SIMULTANEOUS_UPLOADS = 3
    DEFAULT_UPLOAD_MAX_CHUNK_RETRIES = 1

    DEFAULT_DOWNLOAD_FLOW_CHUNK_SIZE = 1_048_576
    FLOW_PERMANENT_ERRORS = [404, 413, 415, 500, 501]

    def upload(
        self,
        local_path: str,
        upload_path: str,
        overwrite: bool = False,
        chunk_size=DEFAULT_UPLOAD_FLOW_CHUNK_SIZE,
        simultaneous_uploads=DEFAULT_UPLOAD_SIMULTANEOUS_UPLOADS,
        max_chunk_retries=DEFAULT_UPLOAD_MAX_CHUNK_RETRIES,
        chunk_retry_interval=1,
    ):
        """Upload a file to the Hopsworks filesystem.

        ```python

        conn = hsml.connection(project="my-project")

        dataset_api = conn.get_dataset_api()

        uploaded_file_path = dataset_api.upload("my_local_file.txt", "Resources")

        ```
        # Arguments
            local_path: local path to file to upload
            upload_path: path to directory where to upload the file in Hopsworks Filesystem
            overwrite: overwrite file if exists
            chunk_size: upload chunk size in megabytes. Default 10 MB
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
        chunk_size_bytes = chunk_size * 1024 * 1024

        if self.path_exists(destination_path):
            if overwrite:
                self.rm(destination_path)
            else:
                raise Exception(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        num_chunks = math.ceil(file_size / chunk_size_bytes)

        base_params = self._get_flow_base_params(
            file_name, num_chunks, file_size, chunk_size_bytes
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
                        chunk = f.read(chunk_size_bytes)
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
        path_params = ["project", _client._project_id, "dataset", "upload", path]

        # Flow configuration params are sent as form data
        _client._send_request(
            "POST", path_params, data=params, files={"file": (file_name, chunk)}
        )

    def download(self, path, local_path):
        """Download file/directory on a path in datasets.
        :param path: path to download
        :type path: str
        :param local_path: path to download in datasets
        :type local_path: str
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "download",
            "with_auth",
            path,
        ]
        query_params = {"type": "DATASET"}

        with _client._send_request(
            "GET", path_params, query_params=query_params, stream=True
        ) as response:
            with open(local_path, "wb") as f:
                downloaded = 0
                # if not response.headers.get("Content-Length"), file is still downloading
                for chunk in response.iter_content(
                    chunk_size=self.DEFAULT_DOWNLOAD_FLOW_CHUNK_SIZE
                ):
                    f.write(chunk)
                    downloaded += len(chunk)

    def get(self, remote_path):
        """Get metadata about a path in datasets.

        :param remote_path: path to check
        :type remote_path: str
        :return: dataset metadata
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def path_exists(self, remote_path):
        """Check if a path exists in datasets.

        :param remote_path: path to check
        :type remote_path: str
        :return: boolean whether path exists
        :rtype: bool
        """
        try:
            self.get(remote_path)
            return True
        except RestAPIError:
            return False

    def list(self, remote_path, sort_by=None, limit=1000):
        """List all files in a directory in datasets.

        :param remote_path: path to list
        :type remote_path: str
        :param sort_by: sort string
        :type sort_by: str
        :param limit: max number of returned files
        :type limit: int
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        query_params = {"action": "listing", "sort_by": sort_by, "limit": limit}
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, headers=headers, query_params=query_params
        )

    def chmod(self, remote_path, permissions):
        """Chmod operation on file or directory in datasets.

        :param remote_path: path to chmod
        :type remote_path: str
        :param permissions: permissions string, for example u+x
        :type permissions: str
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        headers = {"content-type": "application/json"}
        query_params = {"action": "PERMISSION", "permissions": permissions}
        return _client._send_request(
            "PUT", path_params, headers=headers, query_params=query_params
        )

    def mkdir(self, remote_path):
        """Path to create in datasets.

        :param remote_path: path to create
        :type remote_path: str
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        query_params = {
            "action": "create",
            "searchable": "true",
            "generate_readme": "false",
            "type": "DATASET",
        }
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )

    def rm(self, remote_path):
        """Remove a path in datasets.

        :param remote_path: path to remove
        :type remote_path: str
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        return _client._send_request("DELETE", path_params)

    def _archive(
        self,
        remote_path,
        destination_path=None,
        block=False,
        timeout=120,
        action="unzip",
    ):
        """Internal (de)compression logic.

        :param remote_path: path to file or directory to unzip
        :type remote_path: str
        :param destination_path: path to upload the zip
        :type destination_path: str
        :param block: if the operation should be blocking until complete
        :type block: bool
        :param timeout: timeout if the operation is blocking
        :type timeout: int
        :param action: zip or unzip
        :type action: str
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]

        query_params = {"action": action}

        if destination_path is not None:
            query_params["destination_path"] = destination_path
            query_params["destination_type"] = "DATASET"

        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )

        if block is True:
            # Wait for zip file to appear. When it does, check that parent dir zipState is not set to CHOWNING
            count = 0
            while count < timeout:
                if action == "zip":
                    zip_path = remote_path + ".zip"
                    # Get the status of the zipped file
                    if destination_path is None:
                        zip_exists = self.path_exists(zip_path)
                    else:
                        zip_exists = self.path_exists(
                            destination_path + "/" + os.path.split(zip_path)[1]
                        )
                    # Get the zipState of the directory being zipped
                    dir_status = self.get(remote_path)
                    zip_state = (
                        dir_status["zipState"] if "zipState" in dir_status else None
                    )
                    if zip_exists and zip_state == "NONE":
                        return
                    else:
                        time.sleep(1)
                elif action == "unzip":
                    # Get the status of the unzipped dir
                    unzipped_dir_exists = self.path_exists(
                        remote_path[: remote_path.index(".")]
                    )
                    # Get the zipState of the zip being extracted
                    dir_status = self.get(remote_path)
                    zip_state = (
                        dir_status["zipState"] if "zipState" in dir_status else None
                    )
                    if unzipped_dir_exists and zip_state == "NONE":
                        return
                    else:
                        time.sleep(1)
                count += 1
            raise Exception(
                "Timeout of {} seconds exceeded while {} {}.".format(
                    timeout, action, remote_path
                )
            )

    def unzip(self, remote_path, block=False, timeout=120):
        """Unzip an archive in the dataset.

        :param remote_path: path to file or directory to unzip
        :type remote_path: str
        :param block: if the operation should be blocking until complete
        :type block: bool
        :param timeout: timeout if the operation is blocking
        :type timeout: int
        """
        self._archive(remote_path, block=block, timeout=timeout, action="unzip")

    def zip(self, remote_path, destination_path=None, block=False, timeout=120):
        """Zip a file or directory in the dataset.

        :param remote_path: path to file or directory to zip
        :type remote_path: str
        :param destination_path: path to upload the zip
        :type destination_path: str
        :param block: if the operation should be blocking until complete
        :type block: bool
        :param timeout: timeout if the operation is blocking
        :type timeout: int
        """
        self._archive(
            remote_path,
            destination_path=destination_path,
            block=block,
            timeout=timeout,
            action="zip",
        )

    def move(self, source_path, destination_path):
        """Move a file or directory in the dataset.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param source_path: path to file or directory to move
        :type source_path: str
        :param destination_path: destination path
        :type destination_path: str
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", source_path]

        query_params = {"action": "move", "destination_path": destination_path}
        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )

    def copy(self, source_path, destination_path):
        """Copy a file or directory in the dataset.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param source_path: path to file or directory to copy
        :type source_path: str
        :param destination_path: destination path
        :type destination_path: str
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", source_path]

        query_params = {"action": "copy", "destination_path": destination_path}
        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )

    def add(self, path, name, value):
        """Attach a name/value tag to a model.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param path: path to add the tag
        :type path: str
        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added
        :type value: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
            "schema",
            name,
            path,
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def delete(self, path, name):
        """Delete a tag.

        Tag names are unique identifiers.

        :param path: path to delete the tags
        :type path: str
        :param name: name of the tag to be removed
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
            "schema",
            name,
            path,
        ]
        _client._send_request("DELETE", path_params)

    def get_tags(self, path, name: str = None):
        """Get the tags.

        Gets all tags if no tag name is specified.

        :param path: path to get the tags
        :type path: str
        :param name: tag name
        :type name: str
        :return: dict of tag name/values
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
        ]

        if name is not None:
            path_params.append("schema")
            path_params.append(name)
        else:
            path_params.append("all")

        path_params.append(path)

        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }
