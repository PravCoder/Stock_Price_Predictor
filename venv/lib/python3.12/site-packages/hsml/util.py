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

import shutil
import datetime
import inspect
import humps

import numpy as np
import pandas as pd
import os

from urllib.parse import urljoin, urlparse
from json import JSONEncoder, dumps

from hsml import client
from hsml.constants import DEFAULT, PREDICTOR, MODEL

from hsml.tensorflow.model import Model as TFModel
from hsml.torch.model import Model as TorchModel
from hsml.sklearn.model import Model as SkLearnModel
from hsml.python.model import Model as PyModel
from hsml.model import Model as BaseModel

from hsml.predictor import Predictor as BasePredictor
from hsml.tensorflow.predictor import Predictor as TFPredictor
from hsml.torch.predictor import Predictor as TorchPredictor
from hsml.sklearn.predictor import Predictor as SkLearnPredictor
from hsml.python.predictor import Predictor as PyPredictor

from six import string_types


class VersionWarning(Warning):
    pass


class MLEncoder(JSONEncoder):
    def default(self, obj):
        try:
            return obj.to_dict()
        except AttributeError:
            return super().default(obj)


class NumpyEncoder(JSONEncoder):
    """Special json encoder for numpy types.
    Note that some numpy types doesn't have native python equivalence,
    hence json.dumps will raise TypeError.
    In this case, you'll need to convert your numpy types into its closest python equivalence.
    """

    def convert(self, obj):
        import pandas as pd
        import numpy as np
        import base64

        def encode_binary(x):
            return base64.encodebytes(x).decode("ascii")

        if isinstance(obj, np.ndarray):
            if obj.dtype == np.object:
                return [self.convert(x)[0] for x in obj.tolist()]
            elif obj.dtype == np.bytes_:
                return np.vectorize(encode_binary)(obj), True
            else:
                return obj.tolist(), True

        if isinstance(obj, (pd.Timestamp, datetime.date)):
            return obj.isoformat(), True
        if isinstance(obj, bytes) or isinstance(obj, bytearray):
            return encode_binary(obj), True
        if isinstance(obj, np.generic):
            return obj.item(), True
        if isinstance(obj, np.datetime64):
            return np.datetime_as_string(obj), True
        return obj, False

    def default(self, obj):  # pylint: disable=E0202
        res, converted = self.convert(obj)
        if converted:
            return res
        else:
            return super().default(obj)


# Model registry

# - schema and types


def set_model_class(model):
    if "href" in model:
        _ = model.pop("href")
    if "type" in model:  # backwards compatibility
        _ = model.pop("type")
    if "tags" in model:
        _ = model.pop("tags")  # tags are always retrieved from backend

    if "framework" not in model:
        return BaseModel(**model)

    framework = model.pop("framework")
    if framework == MODEL.FRAMEWORK_TENSORFLOW:
        return TFModel(**model)
    if framework == MODEL.FRAMEWORK_TORCH:
        return TorchModel(**model)
    if framework == MODEL.FRAMEWORK_SKLEARN:
        return SkLearnModel(**model)
    elif framework == MODEL.FRAMEWORK_PYTHON:
        return PyModel(**model)


def input_example_to_json(input_example):
    if isinstance(input_example, np.ndarray):
        if input_example.size > 0:
            return _handle_tensor_input(input_example)
        else:
            raise ValueError(
                "input_example of type {} can not be empty".format(type(input_example))
            )
    elif isinstance(input_example, dict):
        return _handle_dict_input(input_example)
    else:
        return _handle_dataframe_input(input_example)


def _handle_tensor_input(input_tensor):
    return input_tensor.tolist()


def _handle_dataframe_input(input_ex):
    if isinstance(input_ex, pd.DataFrame):
        if not input_ex.empty:
            return input_ex.iloc[0].tolist()
        else:
            raise ValueError(
                "input_example of type {} can not be empty".format(type(input_ex))
            )
    elif isinstance(input_ex, pd.Series):
        if not input_ex.empty:
            return input_ex.tolist()
        else:
            raise ValueError(
                "input_example of type {} can not be empty".format(type(input_ex))
            )
    elif isinstance(input_ex, list):
        if len(input_ex) > 0:
            return input_ex
        else:
            raise ValueError(
                "input_example of type {} can not be empty".format(type(input_ex))
            )
    else:
        raise TypeError(
            "{} is not a supported input example type".format(type(input_ex))
        )


def _handle_dict_input(input_ex):
    return input_ex


# - artifacts


def compress(archive_out_path, archive_name, path_to_archive):
    if os.path.isdir(path_to_archive):
        return shutil.make_archive(
            os.path.join(archive_out_path, archive_name), "gztar", path_to_archive
        )
    else:
        return shutil.make_archive(
            os.path.join(archive_out_path, archive_name),
            "gztar",
            os.path.dirname(path_to_archive),
            os.path.basename(path_to_archive),
        )


def decompress(archive_file_path, extract_dir=None):
    return shutil.unpack_archive(archive_file_path, extract_dir=extract_dir)


# - export models


def validate_metrics(metrics):
    if metrics is not None:
        if not isinstance(metrics, dict):
            raise TypeError(
                "provided metrics is of instance {}, expected a dict".format(
                    type(metrics)
                )
            )

        for metric in metrics:
            # Validate key is a string
            if not isinstance(metric, string_types):
                raise TypeError(
                    "provided metrics key is of instance {}, expected a string".format(
                        type(metric)
                    )
                )
            # Validate value is a number
            try:
                float(metrics[metric])
            except ValueError:
                raise ValueError(
                    "{} is not a number, only numbers can be attached as metadata for models.".format(
                        str(metrics[metric])
                    )
                )


# Model serving


def get_predictor_for_model(model, **kwargs):
    if not isinstance(model, BaseModel):
        raise ValueError(
            "model is of type {}, but an instance of {} class is expected".format(
                type(model), BaseModel
            )
        )

    if type(model) == TFModel:
        return TFPredictor(**kwargs)
    if type(model) == TorchModel:
        return TorchPredictor(**kwargs)
    if type(model) == SkLearnModel:
        return SkLearnPredictor(**kwargs)
    if type(model) == PyModel:
        return PyPredictor(**kwargs)
    if type(model) == BaseModel:
        return BasePredictor(  # python as default framework and model server
            model_framework=MODEL.FRAMEWORK_PYTHON,
            model_server=PREDICTOR.MODEL_SERVER_PYTHON,
            **kwargs
        )


def get_hostname_replaced_url(sub_path: str):
    """
    construct and return an url with public hopsworks hostname and sub path
    :param self:
    :param sub_path: url sub-path after base url
    :return: href url
    """
    href = urljoin(client.get_instance()._base_url, sub_path)
    url_parsed = client.get_instance().replace_public_host(urlparse(href))
    return url_parsed.geturl()


# General


def pretty_print(obj):
    if isinstance(obj, list):
        for logs in obj:
            pretty_print(logs)
    else:
        json_decamelized = humps.decamelize(obj.to_dict())
        print(dumps(json_decamelized, indent=4, sort_keys=True))


def get_members(cls, prefix=None):
    for m in inspect.getmembers(cls, lambda m: not (inspect.isroutine(m))):
        n = m[0]  # name
        if (prefix is not None and n.startswith(prefix)) or (
            prefix is None and not (n.startswith("__") and n.endswith("__"))
        ):
            yield m[1]  # value


# - json


def extract_field_from_json(obj, fields, default=None, as_instance_of=None):
    if isinstance(fields, list):
        for field in fields:
            value = extract_field_from_json(obj, field, default, as_instance_of)
            if value is not None:
                break
    else:
        value = obj.pop(fields) if fields in obj else default
        if as_instance_of is not None:
            if isinstance(value, list):
                # if the field is a list, get all obj
                value = [
                    get_obj_from_json(subvalue, as_instance_of) for subvalue in value
                ]
            else:
                # otherwise, get single obj
                value = get_obj_from_json(value, as_instance_of)
    return value


def get_obj_from_json(obj, cls):
    if obj is not None:
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, dict):
            if obj is DEFAULT:
                return cls()
            return cls.from_json(obj)
        raise ValueError(
            "Object of type {} cannot be converted to class {}".format(type(obj), cls)
        )
    return obj
