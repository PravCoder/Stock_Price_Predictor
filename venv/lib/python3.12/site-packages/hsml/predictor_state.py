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

import humps
from typing import Optional

from hsml import util
from hsml.predictor_state_condition import PredictorStateCondition


class PredictorState:
    """State of a predictor."""

    def __init__(
        self,
        available_predictor_instances: int,
        available_transformer_instances: Optional[int],
        hopsworks_inference_path: str,
        model_server_inference_path: str,
        internal_port: Optional[int],
        revision: Optional[int],
        deployed: Optional[bool],
        condition: Optional[PredictorStateCondition],
        status: str,
        **kwargs,
    ):
        self._available_predictor_instances = available_predictor_instances
        self._available_transformer_instances = available_transformer_instances
        self._hopsworks_inference_path = hopsworks_inference_path
        self._model_server_inference_path = model_server_inference_path
        self._internal_port = internal_port
        self._revision = revision
        self._deployed = deployed if deployed is not None else False
        self._condition = condition
        self._status = status

    def describe(self):
        """Print a description of the deployment state"""
        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return PredictorState(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        ai = util.extract_field_from_json(json_decamelized, "available_instances")
        ati = util.extract_field_from_json(
            json_decamelized, "available_transformer_instances"
        )
        hip = util.extract_field_from_json(json_decamelized, "hopsworks_inference_path")
        msip = util.extract_field_from_json(
            json_decamelized, "model_server_inference_path"
        )
        ipt = util.extract_field_from_json(json_decamelized, "internal_port")
        r = util.extract_field_from_json(json_decamelized, "revision")
        d = util.extract_field_from_json(json_decamelized, "deployed")
        c = util.extract_field_from_json(
            json_decamelized, "condition", as_instance_of=PredictorStateCondition
        )
        s = util.extract_field_from_json(json_decamelized, "status")

        return ai, ati, hip, msip, ipt, r, d, c, s

    def to_dict(self):
        json = {
            "availableInstances": self._available_predictor_instances,
            "hopsworksInferencePath": self._hopsworks_inference_path,
            "modelServerInferencePath": self._model_server_inference_path,
            "status": self._status,
        }

        if self._available_transformer_instances is not None:
            json[
                "availableTransformerInstances"
            ] = self._available_transformer_instances
        if self._internal_port is not None:
            json["internalPort"] = self._internal_port
        if self._revision is not None:
            json["revision"] = self._revision
        if self._deployed is not None:
            json["deployed"] = self._deployed
        if self._condition is not None:
            json = {**json, **self._condition.to_dict()}

        return json

    @property
    def available_predictor_instances(self):
        """Available predicotr instances."""
        return self._available_predictor_instances

    @property
    def available_transformer_instances(self):
        """Available transformer instances."""
        return self._available_transformer_instances

    @property
    def hopsworks_inference_path(self):
        """Inference path in the Hopsworks REST API."""
        return self._hopsworks_inference_path

    @property
    def model_server_inference_path(self):
        """Inference path in the model server"""
        return self.model_server_inference_path

    @property
    def internal_port(self):
        """Internal port for the predictor."""
        return self._internal_port

    @property
    def revision(self):
        """Last revision of the predictor."""
        return self._revision

    @property
    def deployed(self):
        """Whether the predictor is deployed or not."""
        return self._deployed

    @property
    def condition(self):
        """Condition of the current state of predictor."""
        return self._condition

    @property
    def status(self):
        """Status of the predictor."""
        return self._status

    def __repr__(self):
        return f"PredictorState(status: {self.status.capitalize()!r})"
