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

import json
import humps

from hopsworks import util


class User:
    def __init__(
        self,
        username=None,
        email=None,
        first_name=None,
        firstname=None,
        last_name=None,
        lastname=None,
        status=None,
        secret=None,
        chosen_password=None,
        repeated_password=None,
        tos=None,
        two_factor=None,
        tours_state=None,
        max_num_projects=None,
        num_created_projects=None,
        test_user=None,
        user_account_type=None,
        num_active_projects=None,
        num_remaining_projects=None,
        href=None,
        **kwargs,
    ):
        self._username = username
        self._email = email

        # This is because we return two different UserDTOs with either the field firstName or firstname
        if first_name is None:
            self._first_name = firstname
        else:
            self._first_name = first_name

        if last_name is None:
            self._last_name = lastname
        else:
            self._last_name = last_name

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def username(self):
        """Username of the user"""
        return self._username

    @property
    def email(self):
        """Email of the user"""
        return self._email

    @property
    def first_name(self):
        """First name of the user"""
        return self._first_name

    @property
    def last_name(self):
        """Last name of the user"""
        return self._last_name

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"User({self._username!r}, {self._email!r}, {self._first_name!r}, {self._last_name!r})"
