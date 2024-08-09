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

import humps
import json

from hopsworks import util


class GitFileStatus:
    def __init__(
        self,
        file=None,
        status=None,
        extra=None,
        **kwargs,
    ):
        self._file = file
        self._status = status
        self._extra = extra

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**file_status) for file_status in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    @property
    def file(self):
        """Path to the file"""
        return self._file

    @property
    def status(self):
        """Status of the file
        Unmodified         StatusCode = ' '
        Untracked          StatusCode = '?'
        Modified           StatusCode = 'M'
        Added              StatusCode = 'A'
        Deleted            StatusCode = 'D'
        Renamed            StatusCode = 'R'
        Copied             StatusCode = 'C'
        UpdatedButUnmerged StatusCode = 'U'"""
        return self._status

    @property
    def extra(self):
        """Extra contains additional information, such as the previous name in a rename"""
        return self._extra

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitFileStatus({self._file!r}, {self._status!r}, {self._extra!r})"
