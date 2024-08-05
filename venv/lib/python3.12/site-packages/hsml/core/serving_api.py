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

import json

from hsml import client, deployment, predictor_state
from hsml import inference_endpoint
from hsml import deployable_component_logs
from hsml.constants import ARTIFACT_VERSION


class ServingApi:
    def __init__(self):
        pass

    def get_by_id(self, id: int):
        """Get the metadata of a deployment with a certain id.

        :param id: id of the deployment
        :type id: int
        :return: deployment metadata object
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            str(id),
        ]
        deployment_json = _client._send_request("GET", path_params)
        return deployment.Deployment.from_response_json(deployment_json)

    def get(self, name: str):
        """Get the metadata of a deployment with a certain name.

        :param name: name of the deployment
        :type name: str
        :return: deployment metadata object
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {"name": name}
        deployment_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        return deployment.Deployment.from_response_json(deployment_json)

    def get_all(self, model_name: str = None, status: str = None):
        """Get the metadata of all deployments.

        :return: model metadata objects
        :rtype: List[Deployment]
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {
            "model": model_name,
            "status": status.capitalize() if status is not None else None,
        }
        deployments_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        return deployment.Deployment.from_response_json(deployments_json)

    def get_inference_endpoints(self):
        """Get inference endpoints.

        :return: inference endpoints for the current project.
        :rtype: List[InferenceEndpoint]
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "inference", "endpoints"]
        endpoints_json = _client._send_request("GET", path_params)
        return inference_endpoint.InferenceEndpoint.from_response_json(endpoints_json)

    def put(self, deployment_instance):
        """Save deployment metadata to model serving.

        :param deployment_instance: metadata object of deployment to be saved
        :type deployment_instance: Deployment
        :return: updated metadata object of the deployment
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        headers = {"content-type": "application/json"}

        if deployment_instance.artifact_version == ARTIFACT_VERSION.CREATE:
            deployment_instance.artifact_version = -1

        return deployment_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                data=deployment_instance.json(),
            )
        )

    def post(self, deployment_instance, action: str):
        """Perform an action on the deployment

        :param action: action to perform on the deployment (i.e., START or STOP)
        :type action: str
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
        ]
        query_params = {"action": action}
        return _client._send_request("POST", path_params, query_params=query_params)

    def delete(self, deployment_instance):
        """Delete the deployment and metadata.

        :param deployment_instance: metadata object of the deployment to delete
        :type deployment_instance: Deployment
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
        ]
        return _client._send_request("DELETE", path_params)

    def get_state(self, deployment_instance):
        """Get the state of a given deployment

        :param deployment_instance: metadata object of the deployment to get state of
        :type deployment_instance: Deployment
        :return: predictor state
        :rtype: PredictorState
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            str(deployment_instance.id),
        ]
        deployment_json = _client._send_request("GET", path_params)
        return predictor_state.PredictorState.from_response_json(deployment_json)

    def reset_changes(self, deployment_instance):
        """Reset a given deployment to the original values in the Hopsworks instance

        :param deployment_instance: metadata object of the deployment to reset
        :type deployment_instance: Deployment
        :return: deployment with reset values
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {"name": deployment_instance.name}
        deployment_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        return deployment_instance.update_from_response_json(deployment_json)

    def send_inference_request(
        self,
        deployment_instance,
        data: dict,
        through_hopsworks: bool = False,
    ):
        """Send inference requests to a deployment with a certain id

        :param deployment_instance: metadata object of the deployment to be used for the prediction
        :type deployment_instance: Deployment
        :param data: payload of the inference requests
        :type data: dict
        :param through_hopsworks: whether to send the inference request through the Hopsworks REST API
        :type through_hopsworks: bool
        :return: inference response
        :rtype: dict
        """

        headers = {"content-type": "application/json"}
        if through_hopsworks:
            # use Hopsworks client
            _client = client.get_instance()
            path_params = self._get_hopsworks_inference_path(
                _client._project_id, deployment_instance
            )
        else:
            _client = client.get_istio_instance()
            if _client is not None:
                # use istio client
                path_params = self._get_istio_inference_path(deployment_instance)
                # - add host header
                headers["host"] = self._get_inference_request_host_header(
                    _client._project_name,
                    deployment_instance.name,
                    client.get_knative_domain(),
                )
            else:
                # fallback to Hopsworks client
                _client = client.get_instance()
                path_params = self._get_hopsworks_inference_path(
                    _client._project_id, deployment_instance
                )
        return _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(data)
        )

    def is_kserve_installed(self):
        """Check if kserve is installed

        :return: whether kserve is installed
        :rtype: bool
        """

        _client = client.get_instance()
        path_params = ["variables", "kube_kserve_installed"]
        kserve_installed = _client._send_request("GET", path_params)
        return (
            "successMessage" in kserve_installed
            and kserve_installed["successMessage"] == "true"
        )

    def get_resource_limits(self):
        """Get resource limits for model serving"""

        _client = client.get_instance()

        path_params = ["variables", "kube_serving_max_cores_allocation"]
        max_cores = _client._send_request("GET", path_params)

        path_params = ["variables", "kube_serving_max_memory_allocation"]
        max_memory = _client._send_request("GET", path_params)

        path_params = ["variables", "kube_serving_max_gpus_allocation"]
        max_gpus = _client._send_request("GET", path_params)

        return {
            "cores": float(max_cores["successMessage"]),
            "memory": int(max_memory["successMessage"]),
            "gpus": int(max_gpus["successMessage"]),
        }

    def get_num_instances_limits(self):
        """Get number of instances limits for model serving"""

        _client = client.get_instance()

        path_params = ["variables", "kube_serving_min_num_instances"]
        min_instances = _client._send_request("GET", path_params)

        path_params = ["variables", "kube_serving_max_num_instances"]
        max_instances = _client._send_request("GET", path_params)

        return [
            int(min_instances["successMessage"]),
            int(max_instances["successMessage"]),
        ]

    def get_knative_domain(self):
        """Get the domain used by knative"""

        _client = client.get_instance()

        path_params = ["variables", "kube_knative_domain_name"]
        domain = _client._send_request("GET", path_params)

        return domain["successMessage"]

    def get_logs(self, deployment_instance, component, tail):
        """Get the logs of a deployment

        :param deployment_instance: metadata object of the deployment to get logs from
        :type deployment_instance: Deployment
        :param component: deployment component (e.g., predictor or transformer)
        :type component: str
        :param tail: number of tailing lines to retrieve
        :type tail: int
        :return: deployment logs
        :rtype: DeployableComponentLogs
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
            "logs",
        ]
        query_params = {"component": component, "tail": tail}
        server_logs = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        return deployable_component_logs.DeployableComponentLogs.from_response_json(
            server_logs
        )

    def _get_inference_request_host_header(
        self, project_name: str, deployment_name: str, domain: str
    ):
        return "{}.{}.{}".format(
            deployment_name, project_name.replace("_", "-"), domain
        ).lower()

    def _get_hopsworks_inference_path(self, project_id: int, deployment_instance):
        return [
            "project",
            project_id,
            "inference",
            "models",
            deployment_instance.name + ":predict",
        ]

    def _get_istio_inference_path(self, deployment_instance):
        return ["v1", "models", deployment_instance.name + ":predict"]
