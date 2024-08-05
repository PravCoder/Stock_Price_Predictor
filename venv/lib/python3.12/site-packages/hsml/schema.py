#
#   Copyright 2021 Logical Clocks AB
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

from hsml.utils.schema.columnar_schema import ColumnarSchema
from hsml.utils.schema.tensor_schema import TensorSchema
import numpy
import pandas
import json
from typing import Optional, Union, TypeVar


class Schema:
    """Create a schema for a model input or output.

    # Arguments
        object: The object to construct the schema from.

    # Returns
        `Schema`. The schema object.
    """

    def __init__(
        self,
        object: Optional[
            Union[
                pandas.DataFrame,
                pandas.Series,
                TypeVar("pyspark.sql.dataframe.DataFrame"),  # noqa: F821
                TypeVar("hsfs.training_dataset.TrainingDataset"),  # noqa: F821
                numpy.ndarray,
                list,
            ]
        ] = None,
        **kwargs,
    ):
        # A tensor schema is either ndarray of a list containing name, type and shape dicts
        if isinstance(object, numpy.ndarray) or (
            isinstance(object, list) and all(["shape" in entry for entry in object])
        ):
            self.tensor_schema = self._convert_tensor_to_schema(object).tensors
        else:
            self.columnar_schema = self._convert_columnar_to_schema(object).columns

    def _convert_columnar_to_schema(self, object):
        return ColumnarSchema(object)

    def _convert_tensor_to_schema(self, object):
        return TensorSchema(object)

    def _get_type(self):
        if hasattr(self, "tensor_schema"):
            return "tensor"
        if hasattr(self, "columnar_schema"):
            return "columnar"
        return None

    def json(self):
        return json.dumps(
            self, default=lambda o: getattr(o, "__dict__", o), sort_keys=True, indent=2
        )

    def to_dict(self):
        """
        Get dict representation of the Schema.
        """
        return json.loads(self.json())

    def __repr__(self):
        return f"Schema(type: {self._get_type()!r})"
