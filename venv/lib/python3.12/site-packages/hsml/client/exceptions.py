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


class RestAPIError(Exception):
    """REST Exception encapsulating the response object and url."""

    def __init__(self, url, response):
        try:
            error_object = response.json()
        except Exception:
            self.error_code = error_object = None

        message = (
            "Metadata operation error: (url: {}). Server response: \n"
            "HTTP code: {}, HTTP reason: {}, body: {}".format(
                url,
                response.status_code,
                response.reason,
                response.content,
            )
        )

        if error_object is not None:
            self.error_code = error_object.get("errorCode", "")
            message += ", error code: {}, error msg: {}, user msg: {}".format(
                self.error_code,
                error_object.get("errorMsg", ""),
                error_object.get("usrMsg", ""),
            )

        super().__init__(message)
        self.url = url
        self.response = response

    STATUS_CODE_BAD_REQUEST = 400
    STATUS_CODE_UNAUTHORIZED = 401
    STATUS_CODE_FORBIDDEN = 403
    STATUS_CODE_NOT_FOUND = 404
    STATUS_CODE_INTERNAL_SERVER_ERROR = 500


class UnknownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter."""


class ModelRegistryException(Exception):
    """Generic model registry exception"""


class ModelServingException(Exception):
    """Generic model serving exception"""

    ERROR_CODE_SERVING_NOT_FOUND = 240000
    ERROR_CODE_ILLEGAL_ARGUMENT = 240001
    ERROR_CODE_DUPLICATED_ENTRY = 240011

    ERROR_CODE_DEPLOYMENT_NOT_RUNNING = 250001


class ExternalClientError(TypeError):
    """Raised when external client cannot be initialized due to missing arguments."""

    def __init__(self, message):
        super().__init__(message)
