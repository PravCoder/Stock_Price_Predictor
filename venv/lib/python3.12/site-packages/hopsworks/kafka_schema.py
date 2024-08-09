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


class KafkaSchema:
    def __init__(
        self,
        id=None,
        subject=None,
        version=None,
        schema=None,
        project_id=None,
        project_name=None,
        type=None,
        **kwargs,
    ):
        self._id = id
        self._subject = subject
        self._version = version
        self._schema = schema

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
    def id(self):
        """Id of the kafka schema"""
        return self._id

    @property
    def subject(self):
        """Name of the subject for the schema"""
        return self._subject

    @property
    def version(self):
        """Version of the schema"""
        return self._version

    @property
    def schema(self):
        """Schema definition"""
        return self._schema

    def delete(self):
        """Delete the schema
        !!! danger "Potentially dangerous operation"
            This operation deletes the schema.
        # Raises
            `RestAPIError`.
        """
        self._kafka_api._delete_subject_version(self.subject, self.version)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"KafkaSchema({self._subject!r}, {self._version!r})"
