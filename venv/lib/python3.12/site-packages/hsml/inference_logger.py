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

import json
import humps
from typing import Union, Optional

from hsml import util

from hsml.constants import DEFAULT, INFERENCE_LOGGER
from hsml.kafka_topic import KafkaTopic


class InferenceLogger:
    """Configuration of an inference logger for a predictor.

    # Arguments
        kafka_topic: Kafka topic to send the inference logs to. By default, a new Kafka topic is configured.
        mode: Inference logging mode. (e.g., `NONE`, `ALL`, `PREDICTIONS`, or `MODEL_INPUTS`). By default, `ALL` inference logs are sent.
    # Returns
        `InferenceLogger`. Configuration of an inference logger.
    """

    def __init__(
        self,
        kafka_topic: Optional[Union[KafkaTopic, dict]] = DEFAULT,
        mode: Optional[str] = INFERENCE_LOGGER.MODE_ALL,
        **kwargs,
    ):
        self._kafka_topic = util.get_obj_from_json(kafka_topic, KafkaTopic)
        self._mode = self._validate_mode(mode) or (
            INFERENCE_LOGGER.MODE_ALL
            if self._kafka_topic is not None
            else INFERENCE_LOGGER.MODE_NONE
        )

    def describe(self):
        """Print a description of the inference logger"""
        util.pretty_print(self)

    def _validate_mode(self, mode):
        if self._kafka_topic is None and mode is not None:
            mode = None
        elif self._kafka_topic is not None and mode is None:
            mode = INFERENCE_LOGGER.MODE_NONE

        if mode is not None:
            modes = list(util.get_members(INFERENCE_LOGGER))
            if mode not in modes:
                raise ValueError(
                    "Inference logging mode '{}' is not valid. Possible values are '{}'".format(
                        mode, ", ".join(modes)
                    )
                )
        return mode

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return InferenceLogger(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        topic = util.extract_field_from_json(
            json_decamelized,
            ["kafka_topic_dto", "kafka_topic"],
            as_instance_of=KafkaTopic,
        )
        mode = util.extract_field_from_json(json_decamelized, "inference_logging")

        return topic, mode

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(*self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        json = {"inferenceLogging": self._mode}
        if self._kafka_topic is not None:
            return {**json, **self._kafka_topic.to_dict()}
        return json

    @property
    def kafka_topic(self):
        """Kafka topic to send the inference logs to."""
        return self._kafka_topic

    @kafka_topic.setter
    def kafka_topic(self, kafka_topic: KafkaTopic):
        self._kafka_topic = kafka_topic

    @property
    def mode(self):
        """Inference logging mode ("NONE", "ALL", "PREDICTIONS", or "MODEL_INPUTS")."""
        return self._mode

    @mode.setter
    def mode(self, mode: str):
        self._mode = mode

    def __repr__(self):
        return f"InferenceLogger(mode: {self._mode!r})"
