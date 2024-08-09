#
#   Copyright 2022 Hopsworks AB
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


class PredictorStateCondition:
    """Condition of a predictor state."""

    def __init__(
        self,
        type: str,
        status: Optional[bool] = None,
        reason: Optional[str] = None,
        **kwargs,
    ):
        self._type = type
        self._status = status
        self._reason = reason

    def describe(self):
        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return PredictorStateCondition(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        type = json_decamelized.pop("type")  # required
        status = util.extract_field_from_json(json_decamelized, "status")
        reason = util.extract_field_from_json(json_decamelized, "reason")

        return type, status, reason

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(*self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        return {
            "condition": {
                "type": self._type,
                "status": self._status,
                "reason": self._reason,
            }
        }

    @property
    def type(self):
        """Condition type of the predictor state."""
        return self._type

    @property
    def status(self):
        """Condition status of the predictor state."""
        return self._status

    @property
    def reason(self):
        """Condition reason of the predictor state."""
        return self._reason

    def __repr__(self):
        return f"PredictorStateCondition(type: {self.type.capitalize()!r}, status: {self.status!r})"
