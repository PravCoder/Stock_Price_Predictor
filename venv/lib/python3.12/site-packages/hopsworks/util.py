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

from json import JSONEncoder
from hopsworks.client.exceptions import JobException
from hopsworks.git_file_status import GitFileStatus
from hopsworks import client
from urllib.parse import urljoin, urlparse


class Encoder(JSONEncoder):
    def default(self, obj):
        try:
            return obj.to_dict()
        except AttributeError:
            return super().default(obj)


def convert_to_abs(path, current_proj_name):
    abs_project_prefix = "/Projects/{}".format(current_proj_name)
    if not path.startswith(abs_project_prefix):
        return abs_project_prefix + "/" + path
    else:
        return path


def validate_job_conf(config, project_name):
    # User is required to set the appPath programmatically after getting the configuration
    if (
        config["type"] != "dockerJobConfiguration"
        and config["type"] != "flinkJobConfiguration"
        and "appPath" not in config
    ):
        raise JobException("'appPath' not set in job configuration")
    elif "appPath" in config and not config["appPath"].startswith("hdfs://"):
        config["appPath"] = "hdfs://" + convert_to_abs(config["appPath"], project_name)

    # If PYSPARK application set the mainClass, if SPARK validate there is a mainClass set
    if config["type"] == "sparkJobConfiguration":
        if config["appPath"].endswith(".py"):
            config["mainClass"] = "org.apache.spark.deploy.PythonRunner"
        elif "mainClass" not in config:
            raise JobException("'mainClass' not set in job configuration")

    return config


def convert_git_status_to_files(files):
    # Convert GitFileStatus to list of file paths
    if isinstance(files[0], GitFileStatus):
        tmp_files = []
        for file_status in files:
            tmp_files.append(file_status.file)
        files = tmp_files

    return files


def get_hostname_replaced_url(sub_path: str):
    """
    construct and return an url with public hopsworks hostname and sub path
    :param self:
    :param sub_path: url sub-path after base url
    :return: href url
    """
    href = urljoin(client.get_instance()._base_url, sub_path)
    url_parsed = client.get_instance().replace_public_host(urlparse(href))
    return url_parsed.geturl()
