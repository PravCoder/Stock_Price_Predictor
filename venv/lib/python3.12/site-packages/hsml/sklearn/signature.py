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

from typing import Optional, Union
import pandas
import numpy
from hsml.model_schema import ModelSchema
from hsml.sklearn.model import Model

_mr = None


def create_model(
    name: str,
    version: Optional[int] = None,
    metrics: Optional[dict] = None,
    description: Optional[str] = None,
    input_example: Optional[
        Union[pandas.DataFrame, pandas.Series, numpy.ndarray, list]
    ] = None,
    model_schema: Optional[ModelSchema] = None,
):
    """Create an SkLearn model metadata object.

    !!! note "Lazy"
        This method is lazy and does not persist any metadata or uploads model artifacts in the
        model registry on its own. To save the model object and the model artifacts, call the `save()` method with a
        local file path to the directory containing the model artifacts.

    # Arguments
        name: Name of the model to create.
        version: Optionally version of the model to create, defaults to `None` and
            will create the model with incremented version from the last
            version in the model registry.
        metrics: Optionally a dictionary with model evaluation metrics (e.g., accuracy, MAE)
        description: Optionally a string describing the model, defaults to empty string
            `""`.
        input_example: Optionally an input example that represents a single input for the model, defaults to `None`.
        model_schema: Optionally a model schema for the model inputs and/or outputs.

    # Returns
        `Model`. The model metadata object.
    """
    model = Model(
        id=None,
        name=name,
        version=version,
        description=description,
        metrics=metrics,
        input_example=input_example,
        model_schema=model_schema,
    )
    model._shared_registry_project_name = _mr.shared_registry_project_name
    model._model_registry_id = _mr.model_registry_id

    return model
