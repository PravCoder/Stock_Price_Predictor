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

from hsml.constants import INFERENCE_BATCHER


class InferenceBatcher:
    """Configuration of an inference batcher for a predictor.

    # Arguments
        enabled: Whether the inference batcher is enabled or not. The default value is `false`.
        max_batch_size: Maximum requests batch size.
        max_latency: Maximum latency for request batching.
        timeout: Maximum waiting time for request batching.
    # Returns
        `InferenceLogger`. Configuration of an inference logger.
    """

    def __init__(
        self,
        enabled: Optional[bool] = None,
        max_batch_size: Optional[int] = None,
        max_latency: Optional[int] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ):
        self._enabled = enabled if enabled is not None else INFERENCE_BATCHER.ENABLED
        self._max_batch_size = max_batch_size if max_batch_size is not None else None
        self._max_latency = max_latency if max_latency is not None else None
        self._timeout = timeout if timeout is not None else None

    def describe(self):
        """Print a description of the inference batcher"""
        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return InferenceBatcher(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        config = (
            json_decamelized.pop("batching_configuration")
            if "batching_configuration" in json_decamelized
            else json_decamelized
        )
        enabled = util.extract_field_from_json(config, ["batching_enabled", "enabled"])
        max_batch_size = util.extract_field_from_json(config, "max_batch_size")
        max_latency = util.extract_field_from_json(config, "max_latency")
        timeout = util.extract_field_from_json(config, "timeout")

        return enabled, max_batch_size, max_latency, timeout

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        json = {"batchingEnabled": self._enabled}
        if self._max_batch_size is not None:
            json["maxBatchSize"] = self._max_batch_size
        if self._max_latency is not None:
            json["maxLatency"] = self._max_latency
        if self._timeout is not None:
            json["timeout"] = self._timeout
        return {"batchingConfiguration": json}

    @property
    def enabled(self):
        """Whether the inference batcher is enabled or not."""
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        self._enabled = enabled

    @property
    def max_batch_size(self):
        """Maximum requests batch size."""
        return self._max_batch_size

    @max_batch_size.setter
    def max_batch_size(self, max_batch_size: int):
        self._max_batch_size = max_batch_size

    @property
    def max_latency(self):
        """Maximum latency."""
        return self._max_latency

    @max_latency.setter
    def max_latency(self, max_latency: int):
        self._max_latency = max_latency

    @property
    def timeout(self):
        """Maximum timeout."""
        return self._timeout

    @timeout.setter
    def timeout(self, timeout: int):
        self._timeout = timeout

    def __repr__(self):
        return f"InferenceBatcher(enabled: {self._enabled!r})"
