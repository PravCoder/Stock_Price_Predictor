#
#   Copyright 2023 Hopsworks AB
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
from unittest import TestCase, mock
from contextlib import contextmanager
import hopsworks
import getpass
import os
import uuid
import tempfile
import importlib
import shutil
from hopsworks.client import exceptions
from hopsworks.project import Project


@contextmanager
def input(*cmds):
    """Replace input."""
    hidden_cmds = [c.get("hidden") for c in cmds if isinstance(c, dict)]
    with mock.patch("getpass.getpass", side_effect=hidden_cmds):
        yield


class TestLogin(TestCase):
    """Test hopsworks login."""

    def setUp(self):
        if not hasattr(self, "system_tmp"):
            importlib.reload(tempfile)
            self.system_tmp = tempfile.gettempdir()

        self.cwd_path = os.path.join(self.system_tmp, str(uuid.uuid4()))
        self.temp_dir = os.path.join(self.cwd_path, "tmp")
        self.home_dir = os.path.join(self.cwd_path, "home")

        os.environ["TMP"] = self.temp_dir
        os.environ["HOME"] = self.home_dir

        os.mkdir(self.cwd_path)
        os.mkdir(self.temp_dir)
        os.mkdir(self.home_dir)

        importlib.reload(tempfile)

        os.chdir(self.cwd_path)

    def tearDown(self):
        os.environ["TMP"] = self.system_tmp
        shutil.rmtree(self.cwd_path)
        hopsworks.logout()

    def _check_api_key_existence(self):

        path = hopsworks._get_cached_api_key_path()

        api_key_name = ".hw_api_key"
        api_key_folder = ".{}_hopsworks_app".format(getpass.getuser())

        # Path for current working directory api key
        cwd_api_key_path = os.path.join(os.getcwd(), api_key_name)

        # Path for home api key
        home_dir_path = os.path.expanduser("~")
        home_api_key_path = os.path.join(home_dir_path, api_key_folder, api_key_name)

        # Path for tmp api key
        temp_dir_path = tempfile.gettempdir()
        temp_api_key_path = os.path.join(temp_dir_path, api_key_folder, api_key_name)

        return (
            path,
            path == cwd_api_key_path,
            path == home_api_key_path,
            path == temp_api_key_path,
        )

    def test_login_api_key_as_input(self):
        # Should accept api key as input from command line

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

        with input({"hidden": os.environ["APP_API_KEY"]}):
            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

    def test_login_api_key_as_argument(self):
        # Should accept api key as argument
        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False
        # Should create API key in home by default
        project = hopsworks.login(api_key_value=os.environ["APP_API_KEY"])
        self.assertTrue(isinstance(project, Project))

        self.api_key_path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

    def test_login_cmd_input_incorrect(self):
        # Should fail to login with incorrect API key

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

        with self.assertRaises(exceptions.RestAPIError):
            with input({"hidden": "incorrect_api_key"}):
                hopsworks.login()

    def test_login_fallback_to_tmp(self):
        # Should fall back to storing api key in tmp folder if home is not write and executable for user
        os.chmod(self.home_dir, 0o400)

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is False
        assert in_tmp is True and not os.path.exists(path)

        # Should use API key in tmp folder
        with input({"hidden": os.environ["APP_API_KEY"]}):
            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

        assert in_cwd is False
        assert in_home is False
        assert in_tmp is True and os.path.exists(path)

    def test_login_use_cwd_api_key(self):
        # Should use API key in cwd if exists

        api_key_path = os.path.join(os.getcwd(), ".hw_api_key")
        with open(api_key_path, "w") as f:
            f.write(os.environ["APP_API_KEY"])

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is True and os.path.exists(path)
        assert in_home is False
        assert in_tmp is False

        project = hopsworks.login()
        self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is True and os.path.exists(path)
        assert in_home is False
        assert in_tmp is False

    def test_login_use_home_api_key(self):
        # Should use API key in home if exists

        api_key_folder_path = os.path.join(
            os.path.expanduser("~"), ".{}_hopsworks_app".format(getpass.getuser())
        )
        os.mkdir(api_key_folder_path)
        with open(os.path.join(api_key_folder_path, ".hw_api_key"), "w") as f:
            f.write(os.environ["APP_API_KEY"])

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

        project = hopsworks.login()
        self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

    def test_login_api_key_as_environ(self):
        # Should accept api key as environmet variable
        try:
            os.environ["HOPSWORKS_API_KEY"] = os.environ["APP_API_KEY"]

            path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

            assert in_cwd is False
            assert in_home is True and not os.path.exists(path)
            assert in_tmp is False

            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

            path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

            assert in_cwd is False
            assert in_home is True and not os.path.exists(path)
            assert in_tmp is False
        except Exception as e:
            raise e
        finally:
            del os.environ["HOPSWORKS_API_KEY"]

    def test_login_newline_in_api_key(self):
        try:
            imaginaryApiKey = "ImaginaryApiKey\n"
            hopsworks.login(api_key_value=imaginaryApiKey)
        except Exception as e:
            self.assertNotIn(imaginaryApiKey.strip(), str(e))
