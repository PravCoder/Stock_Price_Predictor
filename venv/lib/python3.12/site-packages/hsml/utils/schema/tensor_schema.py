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

from hsml.utils.schema.tensor import Tensor
import numpy


class TensorSchema:
    """Metadata object representing a tensor schema for a model."""

    def __init__(self, tensor_obj=None):
        if isinstance(tensor_obj, list):
            self.tensors = self._convert_list_to_schema(tensor_obj)
        elif isinstance(tensor_obj, numpy.ndarray):
            self.tensors = self._convert_tensor_to_schema(tensor_obj)
        else:
            raise TypeError(
                "{} is not supported in a tensor schema.".format(type(tensor_obj))
            )

    def _convert_tensor_to_schema(self, tensor_obj):
        return Tensor(tensor_obj.dtype, tensor_obj.shape)

    def _convert_list_to_schema(self, tensor_obj):
        if len(tensor_obj) == 1:
            return [self._build_tensor(tensor_obj[0])]
        else:
            tensors = []
            for tensor in tensor_obj:
                tensors.append(self._build_tensor(tensor))
        return tensors

    def _build_tensor(self, tensor_obj):
        name = None
        type = None
        shape = None
        description = None

        # Name is optional
        if "name" in tensor_obj:
            name = tensor_obj["name"]

        if "description" in tensor_obj:
            description = tensor_obj["description"]

        if "type" in tensor_obj:
            type = tensor_obj["type"]
        else:
            raise ValueError(
                "Mandatory 'type' key missing from entry {}".format(tensor_obj)
            )

        if "shape" in tensor_obj:
            shape = tensor_obj["shape"]
        else:
            raise ValueError(
                "Mandatory 'shape' key missing from entry {}".format(tensor_obj)
            )

        return Tensor(type, shape, name=name, description=description)
