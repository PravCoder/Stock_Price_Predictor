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
from datetime import datetime

from hsml import util


class DeployableComponentLogs:
    """Server logs of a deployable component (predictor or transformer).

    # Arguments
        name: Deployment instance name.
        content: actual logs
    # Returns
        `DeployableComponentLogs`. Server logs of a deployable component
    """

    def __init__(self, instance_name: str, content: str, **kwargs):
        self._instance_name = instance_name
        self._content = content
        self._created_at = datetime.now()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if len(json_decamelized) == 0:
            return []
        return [cls.from_json(logs) for logs in json_decamelized]

    @classmethod
    def from_json(cls, json_decamelized):
        return DeployableComponentLogs(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        instance_name = util.extract_field_from_json(json_decamelized, "instance_name")
        content = util.extract_field_from_json(json_decamelized, "content")
        return instance_name, content

    def to_dict(self):
        return {"instance_name": self._instance_name, "content": self._content}

    @property
    def instance_name(self):
        """Name of the deployment instance containing these server logs."""
        return self._instance_name

    @property
    def content(self):
        """Content of the server logs of the current deployment instance."""
        return self._content

    @property
    def created_at(self):
        """Datetime when the current server logs chunk was retrieved."""
        return self._created_at

    @property
    def component(self):
        """Component of the deployment containing these server logs."""
        return self._component

    @component.setter
    def component(self, component: str):
        self._component = component

    @property
    def tail(self):
        """Number of lines of server logs."""
        return self._tail

    @tail.setter
    def tail(self, tail: int):
        self._tail = tail

    def __repr__(self):
        return f"DeployableComponentLogs(instance_name: {self._instance_name!r}, date: {self._created_at!r}, lines: {self._lines!r})"
