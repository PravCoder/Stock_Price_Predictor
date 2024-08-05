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

try:
    import pydoop.hdfs as hdfs
except ImportError:
    pass

from hsml import client


class NativeHdfsApi:
    def __init__(self):
        pass

    def exists(self, hdfs_path):
        return hdfs.path.exists(hdfs_path)

    def project_path(self):
        _client = client.get_instance()
        return hdfs.path.abspath("/Projects/" + _client._project_name + "/")

    def chmod(self, hdfs_path, mode):
        return hdfs.chmod(hdfs_path, mode)

    def mkdir(self, path):
        return hdfs.mkdir(path)

    def rm(self, path, recursive=True):
        hdfs.rm(path, recursive=recursive)

    def upload(self, local_path: str, remote_path: str):
        # copy from local fs to hdfs
        hdfs.put(local_path, remote_path)

    def download(self, remote_path: str, local_path: str):
        # copy from hdfs to local fs
        hdfs.get(remote_path, local_path)

    def copy(self, source_path: str, destination_path: str):
        # both paths are hdfs paths
        hdfs.cp(source_path, destination_path)

    def move(self, source_path: str, destination_path: str):
        # both paths are hdfs paths
        hdfs.rename(source_path, destination_path)
