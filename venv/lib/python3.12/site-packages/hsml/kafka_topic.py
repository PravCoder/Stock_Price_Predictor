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
from typing import Optional

from hsml import util
from hsml.constants import KAFKA_TOPIC


class KafkaTopic:
    """Configuration for a Kafka topic."""

    def __init__(
        self,
        name: str = KAFKA_TOPIC.CREATE,
        num_replicas: Optional[int] = None,
        num_partitions: Optional[int] = None,
        **kwargs,
    ):
        self._name = name
        self._num_replicas, self._num_partitions = self._validate_topic_config(
            num_replicas, num_partitions
        )

    def describe(self):
        util.pretty_print(self)

    def _validate_topic_config(self, num_replicas, num_partitions):
        if self._name is not None and self._name != KAFKA_TOPIC.NONE:
            if self._name == KAFKA_TOPIC.CREATE:
                if num_replicas is None:
                    print(
                        "Setting number of replicas to default value '{}'".format(
                            KAFKA_TOPIC.NUM_REPLICAS
                        )
                    )
                    num_replicas = KAFKA_TOPIC.NUM_REPLICAS
                if num_partitions is None:
                    print(
                        "Setting number of partitions to default value '{}'".format(
                            KAFKA_TOPIC.NUM_PARTITIONS
                        )
                    )
                    num_partitions = KAFKA_TOPIC.NUM_PARTITIONS
            else:
                if num_replicas is not None or num_partitions is not None:
                    raise ValueError(
                        "Number of replicas or partitions cannot be changed in existing kafka topics"
                    )
        elif self._name is None or self._name == KAFKA_TOPIC.NONE:
            if num_replicas is not None or num_replicas != 0:
                print("No kafka topic specified. Setting number of replicas to '0'")
                num_replicas = 0
            if num_partitions is not None or num_partitions != 0:
                print("No kafka topic specified. Setting number of partitions to '0'")
                num_partitions = 0

        return num_replicas, num_partitions

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return KafkaTopic(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        name = json_decamelized.pop("name")  # required
        tnr = util.extract_field_from_json(
            json_decamelized, ["num_of_replicas", "num_replicas"]
        )
        tnp = util.extract_field_from_json(
            json_decamelized, ["num_of_partitions", "num_partitions"]
        )

        return name, tnr, tnp

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(*self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        return {
            "kafkaTopicDTO": {
                "name": self._name,
                "numOfReplicas": self._num_replicas,
                "numOfPartitions": self._num_partitions,
            }
        }

    @property
    def name(self):
        """Name of the Kafka topic."""
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def num_replicas(self):
        """Number of replicas of the Kafka topic."""
        return self._num_replicas

    @num_replicas.setter
    def num_replicas(self, num_replicas: int):
        self._num_replicas = num_replicas

    @property
    def num_partitions(self):
        """Number of partitions of the Kafka topic."""
        return self._num_partitions

    @num_partitions.setter
    def topic_num_partitions(self, num_partitions: int):
        self._num_partitions = num_partitions

    def __repr__(self):
        return f"KafkaTopic({self._name!r})"
