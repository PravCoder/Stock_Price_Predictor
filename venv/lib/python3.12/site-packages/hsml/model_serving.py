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

from typing import Union, Optional

from hsml import util

from hsml.constants import ARTIFACT_VERSION, PREDICTOR_STATE
from hsml.core import serving_api
from hsml.model import Model
from hsml.predictor import Predictor
from hsml.deployment import Deployment
from hsml.resources import PredictorResources
from hsml.inference_logger import InferenceLogger
from hsml.inference_batcher import InferenceBatcher
from hsml.transformer import Transformer


class ModelServing:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        project_name: str,
        project_id: int,
        **kwargs,
    ):
        self._project_name = project_name
        self._project_id = project_id

        self._serving_api = serving_api.ServingApi()

    def get_deployment_by_id(self, id: int):
        """Get a deployment by id from Model Serving.
        Getting a deployment from Model Serving means getting its metadata handle
        so you can subsequently operate on it (e.g., start or stop).

        !!! example
            ```python
            # login and get Hopsworks Model Serving handle using .login() and .get_model_serving()

            # get a deployment by id
            my_deployment = ms.get_deployment_by_id(1)
            ```

        # Arguments
            id: Id of the deployment to get.
        # Returns
            `Deployment`: The deployment metadata object.
        # Raises
            `RestAPIError`: If unable to retrieve deployment from model serving.
        """

        return self._serving_api.get_by_id(id)

    def get_deployment(self, name: str):
        """Get a deployment by name from Model Serving.

        !!! example
            ```python
            # login and get Hopsworks Model Serving handle using .login() and .get_model_serving()

            # get a deployment by name
            my_deployment = ms.get_deployment('deployment_name')
            ```

        Getting a deployment from Model Serving means getting its metadata handle
        so you can subsequently operate on it (e.g., start or stop).

        # Arguments
            name: Name of the deployment to get.
        # Returns
            `Deployment`: The deployment metadata object.
        # Raises
            `RestAPIError`: If unable to retrieve deployment from model serving.
        """

        return self._serving_api.get(name)

    def get_deployments(self, model: Model = None, status: str = None):
        """Get all deployments from model serving.
        !!! example
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            list_deployments = ms.get_deployment(my_model)

            for deployment in list_deployments:
                print(deployment.get_state())
            ```
        # Arguments
            model: Filter by model served in the deployments
            status: Filter by status of the deployments
        # Returns
            `List[Deployment]`: A list of deployments.
        # Raises
            `RestAPIError`: If unable to retrieve deployments from model serving.
        """

        model_name = model.name if model is not None else None
        if status is not None:
            self._validate_deployment_status(status)

        return self._serving_api.get_all(model_name, status)

    def _validate_deployment_status(self, status):
        statuses = list(util.get_members(PREDICTOR_STATE, prefix="STATUS"))
        status = status.upper()
        if status not in statuses:
            raise ValueError(
                "Deployment status '{}' is not valid. Possible values are '{}'".format(
                    status, ", ".join(statuses)
                )
            )
        return status

    def get_inference_endpoints(self):
        """Get all inference endpoints available in the current project.

        # Returns
            `List[InferenceEndpoint]`: Inference endpoints for model inference
        """

        return self._serving_api.get_inference_endpoints()

    def create_predictor(
        self,
        model: Model,
        name: Optional[str] = None,
        artifact_version: Optional[str] = ARTIFACT_VERSION.CREATE,
        serving_tool: Optional[str] = None,
        script_file: Optional[str] = None,
        resources: Optional[Union[PredictorResources, dict]] = None,
        inference_logger: Optional[Union[InferenceLogger, dict, str]] = None,
        inference_batcher: Optional[Union[InferenceBatcher, dict]] = None,
        transformer: Optional[Union[Transformer, dict]] = None,
    ):
        """Create a Predictor metadata object.

        !!! example
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = my_predictor.deploy()
            ```

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or deploy any model on its own.
            To create a deployment using this predictor, call the `deploy()` method.

        # Arguments
            model: Model to be deployed.
            name: Name of the predictor.
            artifact_version: Version number of the model artifact to deploy, `CREATE` to create a new model artifact
            or `MODEL-ONLY` to reuse the shared artifact containing only the model files.
            serving_tool: Serving tool used to deploy the model server.
            script_file: Path to a custom predictor script implementing the Predict class.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            transformer: Transformer to be deployed together with the predictor.

        # Returns
            `Predictor`. The predictor metadata object.
        """

        if name is None:
            name = model.name

        return Predictor.for_model(
            model,
            name=name,
            artifact_version=artifact_version,
            serving_tool=serving_tool,
            script_file=script_file,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            transformer=transformer,
        )

    def create_transformer(
        self,
        script_file: Optional[str] = None,
        resources: Optional[Union[PredictorResources, dict]] = None,
    ):
        """Create a Transformer metadata object.

        !!! example
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Dataset API instance
            dataset_api = project.get_dataset_api()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # create my_transformer.py Python script
            class Transformer(object):
                def __init__(self):
                    ''' Initialization code goes here '''
                    pass

                def preprocess(self, inputs):
                    ''' Transform the requests inputs here. The object returned by this method will be used as model input to make predictions. '''
                    return inputs

                def postprocess(self, outputs):
                    ''' Transform the predictions computed by the model before returning a response '''
                    return outputs

            uploaded_file_path = dataset_api.upload("my_transformer.py", "Resources", overwrite=True)
            transformer_script_path = os.path.join("/Projects", project.name, uploaded_file_path)

            my_transformer = ms.create_transformer(script_file=uploaded_file_path)

            # or

            from hsml.transformer import Transformer

            my_transformer = Transformer(script_file)
            ```

        !!! example "Create a deployment with the transformer"
            ```python

            my_predictor = ms.create_predictor(transformer=my_transformer)
            my_deployment = my_predictor.deploy()

            # or
            my_deployment = ms.create_deployment(my_predictor, transformer=my_transformer)
            my_deployment.save()
            ```

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or deploy any transformer. To create a deployment using this transformer, set it in the `predictor.transformer` property.

        # Arguments
            script_file: Path to a custom predictor script implementing the Transformer class.
            resources: Resources to be allocated for the transformer.

        # Returns
            `Transformer`. The model metadata object.
        """

        return Transformer(script_file=script_file, resources=resources)

    def create_deployment(self, predictor: Predictor, name: Optional[str] = None):
        """Create a Deployment metadata object.

        !!! example
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = ms.create_deployment(my_predictor)
            my_deployment.save()
            ```

        !!! example "Using the model object"
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            my_deployment = my_model.deploy()

            my_deployment.get_state().describe()
            ```

        !!! example "Using the Model Serving handle"
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = my_predictor.deploy()

            my_deployment.get_state().describe()
            ```

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or deploy any model. To create a deployment, call the `save()` method.

        # Arguments
            predictor: predictor to be used in the deployment
            name: name of the deployment

        # Returns
            `Deployment`. The model metadata object.
        """

        return Deployment(predictor=predictor, name=name)

    @property
    def project_name(self):
        """Name of the project in which Model Serving is located."""
        return self._project_name

    @property
    def project_path(self):
        """Path of the project the registry is connected to."""
        return "/Projects/{}".format(self._project_name)

    @property
    def project_id(self):
        """Id of the project in which Model Serving is located."""
        return self._project_id

    def __repr__(self):
        return f"ModelServing(project: {self._project_name!r})"
