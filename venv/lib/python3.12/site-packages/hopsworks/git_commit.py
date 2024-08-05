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

from hopsworks import util
import humps
import json


class GitCommit:
    def __init__(
        self,
        name=None,
        email=None,
        message=None,
        commit_hash=None,
        time=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._name = name
        self._email = email
        self._message = message
        self._hash = commit_hash
        self._time = time
        self._type = type
        self._href = href
        self._expand = expand
        self._items = items
        self._count = count

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            if "count" in json_decamelized:
                if json_decamelized["count"] == 0:
                    return []
                return [cls(**commit) for commit in json_decamelized["items"]]
            else:
                return cls(**json_decamelized)
        else:
            return None

    @property
    def name(self):
        """Name of the user"""
        return self._name

    @property
    def email(self):
        """Email of the user"""
        return self._email

    @property
    def message(self):
        """Commit message"""
        return self._message

    @property
    def hash(self):
        """Commit hash"""
        return self._hash

    @property
    def time(self):
        """Timestamp for the commit"""
        return self._time

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitCommit({self._name!r}, {self._message!r}, {self._hash!r})"
