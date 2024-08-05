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

import random
import humps
from typing import List, Optional

from hsml import util


class InferenceEndpointPort:
    """Port of an inference endpoint.

    # Arguments
        name: Name of the port. It typically defines the purpose of the port (e.g., HTTP, HTTPS, STATUS-PORT, TLS)
        number: Port number.
    # Returns
        `InferenceEndpointPort`. Port of an inference endpoint.
    """

    def __init__(self, name: str, number: int, **kwargs):
        self._name = name
        self._number = number

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return InferenceEndpointPort(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        name = util.extract_field_from_json(json_decamelized, "name")
        number = util.extract_field_from_json(json_decamelized, "number")
        return name, number

    def to_dict(self):
        return {"name": self._name, "number": self._number}

    @property
    def name(self):
        """Name of the inference endpoint port."""
        return self._name

    @property
    def number(self):
        """Port number of the inference endpoint port."""
        return self._number

    def __repr__(self):
        return f"InferenceEndpointPort(name: {self._name!r})"


class InferenceEndpoint:
    """Inference endpoint available in the current project for model inference.

    # Arguments
        type: Type of inference endpoint (e.g., NODE, KUBE_CLUSTER, LOAD_BALANCER).
        hosts: List of hosts of the inference endpoint.
        ports: List of ports of the inference endpoint.
    # Returns
        `InferenceEndpoint`. Inference endpoint.
    """

    def __init__(
        self,
        type: str,
        hosts: List[str],
        ports: Optional[List[InferenceEndpointPort]],
    ):
        self._type = type
        self._hosts = hosts
        self._ports = ports

    def get_any_host(self):
        """Get any host available"""
        return random.choice(self._hosts) if self._hosts is not None else None

    def get_port(self, name):
        """Get port by name"""
        for port in self._ports:
            if port.name == name:
                return port
        return None

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, list):
            if len(json_decamelized) == 0:
                return []
            return [cls.from_json(endpoint) for endpoint in json_decamelized]
        else:
            return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return InferenceEndpoint(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        type = util.extract_field_from_json(json_decamelized, "type")
        hosts = util.extract_field_from_json(json_decamelized, "hosts")
        ports = util.extract_field_from_json(
            json_decamelized, "ports", as_instance_of=InferenceEndpointPort
        )
        return type, hosts, ports

    def to_dict(self):
        return {
            "type": self._type,
            "hosts": self._hosts,
            "ports": [port.to_dict() for port in self._ports],
        }

    @property
    def type(self):
        """Type of inference endpoint."""
        return self._type

    @property
    def hosts(self):
        """Hosts of the inference endpoint."""
        return self._hosts

    @property
    def ports(self):
        """Ports of the inference endpoint."""
        return self._ports

    def __repr__(self):
        return f"InferenceEndpoint(type: {self._type!r})"


def get_endpoint_by_type(endpoints, type) -> InferenceEndpoint:
    for endpoint in endpoints:
        if endpoint.type == type:
            return endpoint
    return None
