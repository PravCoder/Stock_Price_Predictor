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

from hsml.core import native_hdfs_api, model_api
from hsml import client


class HopsworksEngine:
    def __init__(self):
        self._native_hdfs_api = native_hdfs_api.NativeHdfsApi()
        self._model_api = model_api.ModelApi()

    def mkdir(self, remote_path: str):
        remote_path = self._prepend_project_path(remote_path)
        self._native_hdfs_api.mkdir(remote_path)
        self._native_hdfs_api.chmod(remote_path, "ug+rwx")

    def delete(self, model_instance):
        self._model_api.delete(model_instance)

    def upload(self, local_path: str, remote_path: str, upload_configuration=None):
        local_path = self._get_abs_path(local_path)
        remote_path = self._prepend_project_path(remote_path)
        self._native_hdfs_api.upload(local_path, remote_path)
        self._native_hdfs_api.chmod(remote_path, "ug+rwx")

    def download(self, remote_path: str, local_path: str):
        local_path = self._get_abs_path(local_path)
        remote_path = self._prepend_project_path(remote_path)
        self._native_hdfs_api.download(remote_path, local_path)

    def copy(self, source_path: str, destination_path: str):
        # both paths are hdfs paths
        source_path = self._prepend_project_path(source_path)
        destination_path = self._prepend_project_path(destination_path)
        self._native_hdfs_api.copy(source_path, destination_path)

    def move(self, source_path: str, destination_path: str):
        source_path = self._prepend_project_path(source_path)
        destination_path = self._prepend_project_path(destination_path)
        self._native_hdfs_api.move(source_path, destination_path)

    def _get_abs_path(self, local_path: str):
        return local_path if os.path.isabs(local_path) else os.path.abspath(local_path)

    def _prepend_project_path(self, remote_path: str):
        if not remote_path.startswith("/Projects/"):
            _client = client.get_instance()
            remote_path = "/Projects/{}/{}".format(_client._project_name, remote_path)
        return remote_path
