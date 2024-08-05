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

from typing import Union, Optional

from hsml import client, util
from hsml import predictor as predictor_mod

from hsml.core import serving_api
from hsml.engine import serving_engine
from hsml.predictor_state import PredictorState
from hsml.resources import Resources
from hsml.inference_logger import InferenceLogger
from hsml.inference_batcher import InferenceBatcher
from hsml.transformer import Transformer

from hsml.client.exceptions import ModelServingException
from hsml.constants import DEPLOYABLE_COMPONENT, PREDICTOR_STATE


class Deployment:
    """Metadata object representing a deployment in Model Serving."""

    def __init__(
        self,
        predictor,
        name: Optional[str] = None,
        description: Optional[str] = None,
        **kwargs,
    ):
        self._predictor = predictor
        self._description = description

        if self._predictor is None:
            raise ModelServingException("A predictor is required")
        elif not isinstance(self._predictor, predictor_mod.Predictor):
            raise ValueError(
                "The predictor provided is not an instance of the Predictor class"
            )

        if name is not None:
            self._predictor.name = name

        if self._description is None:
            self._description = self._predictor.description
        else:
            self._description = self._predictor.description = description

        self._serving_api = serving_api.ServingApi()
        self._serving_engine = serving_engine.ServingEngine()

    def save(self, await_update: Optional[int] = 60):
        """Persist this deployment including the predictor and metadata to Model Serving.

        # Arguments
            await_update: If the deployment is running, awaiting time (seconds) for the running instances to be updated.
                          If the running instances are not updated within this timespan, the call to this method returns while
                          the update in the background.
        """

        self._serving_engine.save(self, await_update)

    def start(self, await_running: Optional[int] = 60):
        """Start the deployment

        # Arguments
            await_running: Awaiting time (seconds) for the deployment to start.
                           If the deployment has not started within this timespan, the call to this method returns while
                           it deploys in the background.
        """

        self._serving_engine.start(self, await_status=await_running)

    def stop(self, await_stopped: Optional[int] = 60):
        """Stop the deployment

        # Arguments
            await_stopped: Awaiting time (seconds) for the deployment to stop.
                           If the deployment has not stopped within this timespan, the call to this method returns while
                           it stopping in the background.
        """

        self._serving_engine.stop(self, await_status=await_stopped)

    def delete(self, force=False):
        """Delete the deployment

        # Arguments
            force: Force the deletion of the deployment.
                   If the deployment is running, it will be stopped and deleted automatically.
                   !!! warn A call to this method does not ask for a second confirmation.
        """

        self._serving_engine.delete(self, force)

    def get_state(self) -> PredictorState:
        """Get the current state of the deployment

        # Returns
            `PredictorState`. The state of the deployment.
        """

        return self._serving_engine.get_state(self)

    def is_created(self) -> bool:
        """Check whether the deployment is created.

        # Returns
            `bool`. Whether the deployment is created or not.
        """

        return (
            self._serving_engine.get_state(self).status
            != PREDICTOR_STATE.STATUS_CREATING
        )

    def is_running(self, or_idle=True, or_updating=True) -> bool:
        """Check whether the deployment is ready to handle inference requests

        # Arguments
            or_idle: Whether the idle state is considered as running (default is True)
            or_updating: Whether the updating state is considered as running (default is True)

        # Returns
            `bool`. Whether the deployment is ready or not.
        """

        status = self._serving_engine.get_state(self).status
        return (
            status == PREDICTOR_STATE.STATUS_RUNNING
            or (or_idle and status == PREDICTOR_STATE.STATUS_IDLE)
            or (or_updating and status == PREDICTOR_STATE.STATUS_UPDATING)
        )

    def is_stopped(self, or_created=True) -> bool:
        """Check whether the deployment is stopped

        # Arguments
            or_created: Whether the creating and created state is considered as stopped (default is True)

        # Returns
            `bool`. Whether the deployment is stopped or not.
        """

        status = self._serving_engine.get_state(self).status
        return status == PREDICTOR_STATE.STATUS_STOPPED or (
            or_created
            and (
                status == PREDICTOR_STATE.STATUS_CREATING
                or status == PREDICTOR_STATE.STATUS_CREATED
            )
        )

    def predict(self, data: dict = None, inputs: list = None):
        """Send inference requests to the deployment.
           One of data or inputs parameters must be set. If both are set, inputs will be ignored.

        !!! example
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # retrieve deployment by name
            my_deployment = ms.get_deployment("my_deployment")

            # (optional) retrieve model input example
            my_model = project.get_model_registry()  \
                              .get_model(my_deployment.model_name, my_deployment.model_version)

            # make predictions using model inputs (single or batch)
            predictions = my_deployment.predict(inputs=my_model.input_example)

            # or using more sophisticated inference request payloads
            data = { "instances": [ my_model.input_example ], "key2": "value2" }
            predictions = my_deployment.predict(data)
            ```

        # Arguments
            data: Payload dictionary for the inference request including the model input(s)
            inputs: Model inputs used in the inference requests

        # Returns
            `dict`. Inference response.
        """

        return self._serving_engine.predict(self, data, inputs)

    def download_artifact(self):
        """Download the model artifact served by the deployment"""

        return self._serving_engine.download_artifact(self)

    def get_logs(self, component="predictor", tail=10):
        """Prints the deployment logs of the predictor or transformer.

        # Arguments
            component: Deployment component to get the logs from (e.g., predictor or transformer)
            tail: Number of most recent lines to retrieve from the logs.
        """

        # validate component
        components = list(util.get_members(DEPLOYABLE_COMPONENT))
        if component not in components:
            raise ValueError(
                "Component '{}' is not valid. Possible values are '{}'".format(
                    component, ", ".join(components)
                )
            )

        logs = self._serving_engine.get_logs(self, component, tail)
        if logs is not None:
            for log in logs:
                print("Instance name: " + log.instance_name)
                print(log.content, end="\n\n")

    def get_url(self):
        """Get url to the deployment in Hopsworks"""

        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/deployments/"
            + str(self.id)
        )
        return util.get_hostname_replaced_url(path)

    def describe(self):
        """Print a description of the deployment"""

        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        predictors = predictor_mod.Predictor.from_response_json(json_dict)
        if isinstance(predictors, list):
            return [
                cls.from_predictor(predictor_instance)
                for predictor_instance in predictors
            ]
        else:
            return cls.from_predictor(predictors)

    @classmethod
    def from_predictor(cls, predictor_instance):
        return Deployment(
            predictor=predictor_instance,
            name=predictor_instance._name,
            description=predictor_instance._description,
        )

    def update_from_response_json(self, json_dict):
        self._predictor.update_from_response_json(json_dict)
        self.__init__(
            predictor=self._predictor,
            name=self._predictor._name,
            description=self._predictor._description,
        )
        return self

    def json(self):
        return self._predictor.json()

    def to_dict(self):
        return self._predictor.to_dict()

    # Deployment

    @property
    def id(self):
        """Id of the deployment."""
        return self._predictor.id

    @property
    def name(self):
        """Name of the deployment."""
        return self._predictor.name

    @name.setter
    def name(self, name: str):
        self._predictor.name = name

    @property
    def description(self):
        """Description of the deployment."""
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description

    @property
    def predictor(self):
        """Predictor used in the deployment."""
        return self._predictor

    @predictor.setter
    def predictor(self, predictor):
        self._predictor = predictor

    @property
    def requested_instances(self):
        """Total number of requested instances in the deployment."""
        return self._predictor.requested_instances

    # Single predictor

    @property
    def model_name(self):
        """Name of the model deployed by the predictor"""
        return self._predictor.model_name

    @model_name.setter
    def model_name(self, model_name: str):
        self._predictor.model_name = model_name

    @property
    def model_path(self):
        """Model path deployed by the predictor."""
        return self._predictor.model_path

    @model_path.setter
    def model_path(self, model_path: str):
        self._predictor.model_path = model_path

    @property
    def model_version(self):
        """Model version deployed by the predictor."""
        return self._predictor.model_version

    @model_version.setter
    def model_version(self, model_version: int):
        self._predictor.model_version = model_version

    @property
    def artifact_version(self):
        """Artifact version deployed by the predictor."""
        return self._predictor.artifact_version

    @artifact_version.setter
    def artifact_version(self, artifact_version: Union[int, str]):
        self._predictor.artifact_version = artifact_version

    @property
    def artifact_path(self):
        """Path of the model artifact deployed by the predictor."""
        return self._predictor.artifact_path

    @property
    def model_server(self):
        """Model server ran by the predictor."""
        return self._predictor.model_server

    @model_server.setter
    def model_server(self, model_server: str):
        self._predictor.model_server = model_server

    @property
    def serving_tool(self):
        """Serving tool used to run the model server."""
        return self._predictor.serving_tool

    @serving_tool.setter
    def serving_tool(self, serving_tool: str):
        self._predictor.serving_tool = serving_tool

    @property
    def script_file(self):
        """Script file used by the predictor."""
        return self._predictor.script_file

    @script_file.setter
    def script_file(self, script_file: str):
        self._predictor.script_file = script_file

    @property
    def resources(self):
        """Resource configuration for the predictor."""
        return self._predictor.resources

    @resources.setter
    def resources(self, resources: Resources):
        self._predictor.resources = resources

    @property
    def inference_logger(self):
        """Configuration of the inference logger attached to this predictor."""
        return self._predictor.inference_logger

    @inference_logger.setter
    def inference_logger(self, inference_logger: InferenceLogger):
        self._predictor.inference_logger = inference_logger

    @property
    def inference_batcher(self):
        """Configuration of the inference batcher attached to this predictor."""
        return self._predictor.inference_batcher

    @inference_batcher.setter
    def inference_batcher(self, inference_batcher: InferenceBatcher):
        self._predictor.inference_batcher = inference_batcher

    @property
    def transformer(self):
        """Transformer configured in the predictor."""
        return self._predictor.transformer

    @transformer.setter
    def transformer(self, transformer: Transformer):
        self._predictor.transformer = transformer

    @property
    def created_at(self):
        """Created at date of the predictor."""
        return self._predictor.created_at

    @property
    def creator(self):
        """Creator of the predictor."""
        return self._predictor.creator

    def __repr__(self):
        desc = (
            f", description: {self._description!r}"
            if self._description is not None
            else ""
        )
        return f"Deployment(name: {self._predictor._name!r}" + desc + ")"
