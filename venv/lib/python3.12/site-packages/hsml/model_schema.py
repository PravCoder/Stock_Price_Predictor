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

from typing import Optional
import json

from hsml.schema import Schema


class ModelSchema:
    """Create a schema for a model.

    # Arguments
        input_schema: Schema to describe the inputs.
        output_schema: Schema to describe the outputs.

    # Returns
        `ModelSchema`. The model schema object.
    """

    def __init__(
        self,
        input_schema: Optional[Schema] = None,
        output_schema: Optional[Schema] = None,
        **kwargs,
    ):
        if input_schema is not None:
            self.input_schema = input_schema

        if output_schema is not None:
            self.output_schema = output_schema

    def json(self):
        return json.dumps(
            self, default=lambda o: getattr(o, "__dict__", o), sort_keys=True, indent=2
        )

    def to_dict(self):
        """
        Get dict representation of the ModelSchema.
        """
        return json.loads(self.json())

    def __repr__(self):
        input_type = (
            self.input_schema._get_type() if hasattr(self, "input_schema") else None
        )
        output_type = (
            self.output_schema._get_type() if hasattr(self, "output_schema") else None
        )
        return f"ModelSchema(input: {input_type!r}, output: {output_type!r})"
