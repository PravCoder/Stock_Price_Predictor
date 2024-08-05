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

from hsml import client
from hsml.model_registry import ModelRegistry
from hsml.core import dataset_api
from hsml.client.exceptions import ModelRegistryException


class ModelRegistryApi:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()

    def get(self, project=None):
        """Get model registry for specific project.
        :param project: project of the model registry
        :type project: str
        :return: the model registry metadata
        :rtype: ModelRegistry
        """
        _client = client.get_instance()

        model_registry_id = _client._project_id
        shared_registry_project_name = None

        # In the case of shared model registry, validate that there is Models dataset shared to the connected project from the set project name
        if project is not None:
            path_params = ["project", _client._project_id, "modelregistries"]
            model_registries = _client._send_request("GET", path_params)
            for registry in model_registries["items"]:
                if registry["name"] == project:
                    model_registry_id = registry["id"]
                    shared_registry_project_name = project

            if shared_registry_project_name is None:
                raise ModelRegistryException(
                    "No model registry shared with current project {}, from project {}".format(
                        _client._project_name, project
                    )
                )
        # In the case of default model registry, validate that there is a Models dataset in the connected project
        elif project is None and not self._dataset_api.path_exists("Models"):
            raise ModelRegistryException(
                "No Models dataset exists in project {}, Please enable the Serving service or create the dataset manually.".format(
                    _client._project_name
                )
            )

        return ModelRegistry(
            _client._project_name,
            _client._project_id,
            model_registry_id,
            shared_registry_project_name=shared_registry_project_name,
        )
