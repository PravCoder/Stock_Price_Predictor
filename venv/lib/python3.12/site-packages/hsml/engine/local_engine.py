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

import os

from hsml.core import dataset_api, model_api
from hsml import client


class LocalEngine:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._model_api = model_api.ModelApi()

    def mkdir(self, remote_path: str):
        remote_path = self._prepend_project_path(remote_path)
        self._dataset_api.mkdir(remote_path)

    def delete(self, model_instance):
        self._model_api.delete(model_instance)

    def upload(self, local_path: str, remote_path: str, upload_configuration=None):
        local_path = self._get_abs_path(local_path)
        remote_path = self._prepend_project_path(remote_path)

        # Initialize the upload configuration to empty dictionary if is None
        upload_configuration = upload_configuration if upload_configuration else {}
        self._dataset_api.upload(
            local_path,
            remote_path,
            chunk_size=upload_configuration.get(
                "chunk_size", self._dataset_api.DEFAULT_UPLOAD_FLOW_CHUNK_SIZE
            ),
            simultaneous_uploads=upload_configuration.get(
                "simultaneous_uploads",
                self._dataset_api.DEFAULT_UPLOAD_SIMULTANEOUS_UPLOADS,
            ),
            max_chunk_retries=upload_configuration.get(
                "max_chunk_retries",
                self._dataset_api.DEFAULT_UPLOAD_MAX_CHUNK_RETRIES,
            ),
        )

    def download(self, remote_path: str, local_path: str):
        local_path = self._get_abs_path(local_path)
        remote_path = self._prepend_project_path(remote_path)
        self._dataset_api.download(remote_path, local_path)

    def copy(self, source_path, destination_path):
        source_path = self._prepend_project_path(source_path)
        destination_path = self._prepend_project_path(destination_path)
        self._dataset_api.copy(source_path, destination_path)

    def move(self, source_path, destination_path):
        source_path = self._prepend_project_path(source_path)
        destination_path = self._prepend_project_path(destination_path)
        self._dataset_api.move(source_path, destination_path)

    def _get_abs_path(self, local_path: str):
        return local_path if os.path.isabs(local_path) else os.path.abspath(local_path)

    def _prepend_project_path(self, remote_path: str):
        if not remote_path.startswith("/Projects/"):
            _client = client.get_instance()
            remote_path = "/Projects/{}/{}".format(_client._project_name, remote_path)
        return remote_path
