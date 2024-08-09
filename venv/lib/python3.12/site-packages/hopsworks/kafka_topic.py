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

import humps
import json
from hopsworks.core import kafka_api
from hopsworks import util


class KafkaTopic:
    def __init__(
        self,
        name=None,
        num_of_replicas=None,
        num_of_partitions=None,
        schema_name=None,
        schema_version=None,
        schema_content=None,
        owner_project_id=None,
        shared=None,
        accepted=None,
        project_id=None,
        project_name=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._name = name
        self._num_of_replicas = num_of_replicas
        self._num_of_partitions = num_of_partitions
        self._schema_name = schema_name
        self._schema_version = schema_version
        self._schema_content = schema_content
        self._owner_project_id = owner_project_id
        self._shared = shared
        self._accepted = accepted

        self._kafka_api = kafka_api.KafkaApi(project_id, project_name)

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized:
            return cls(
                **json_decamelized, project_id=project_id, project_name=project_name
            )
        elif json_decamelized["count"] == 0:
            return []
        else:
            return [
                cls(**kafka_topic, project_id=project_id, project_name=project_name)
                for kafka_topic in json_decamelized["items"]
            ]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    @property
    def name(self):
        """Name of the topic"""
        return self._name

    @property
    def replicas(self):
        """Replication factor for the topic"""
        return self._num_of_replicas

    @property
    def partitions(self):
        """Number of partitions for the topic"""
        return self._num_of_partitions

    @property
    def schema(self):
        """Schema for the topic"""
        return self._kafka_api._get_schema_details(
            self._schema_name, self._schema_version
        )

    def delete(self):
        """Delete the topic
        !!! danger "Potentially dangerous operation"
            This operation deletes the topic.
        # Raises
            `RestAPIError`.
        """
        self._kafka_api._delete_topic(self.name)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"KafkaTopic({self._name!r})"
