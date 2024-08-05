#
#   Copyright 2022 Hopsworks AB
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
import os

from hopsworks import command, util
from hopsworks.core import environment_api, library_api
from hopsworks.engine import environment_engine


class Environment:
    def __init__(
        self,
        python_version,
        python_conflicts,
        pip_search_enabled,
        conflicts=None,
        conda_channel=None,
        libraries=None,
        commands=None,
        href=None,
        type=None,
        project_id=None,
        project_name=None,
        **kwargs,
    ):
        self._python_version = python_version
        self._python_conflicts = python_conflicts
        self._pip_search_enabled = pip_search_enabled
        self._conflicts = conflicts
        self._libraries = libraries
        self._commands = (
            command.Command.from_response_json(commands) if commands else None
        )
        self._project_id = project_id
        self._project_name = project_name

        self._environment_engine = environment_engine.EnvironmentEngine(project_id)
        self._library_api = library_api.LibraryApi(project_id, project_name)
        self._environment_api = environment_api.EnvironmentApi(project_id, project_name)

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            return [
                cls(
                    **env,
                    project_id=project_id,
                    project_name=project_name,
                )
                for env in json_decamelized["items"]
            ]
        else:
            return cls(
                **json_decamelized,
                project_id=project_id,
                project_name=project_name,
            )

    @property
    def python_version(self):
        """Python version of the environment"""
        return self._python_version

    def install_wheel(self, path, await_installation=True):
        """Install a python library packaged in a wheel file

        ```python

        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        whl_path = ds_api.upload("matplotlib-3.1.3-cp38-cp38-manylinux1_x86_64.whl", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment()
        env.install_wheel(whl_path)

        ```

        # Arguments
            path: str. The path on Hopsworks where the wheel file is located
            await_installation: bool. If True the method returns only when the installation finishes. Default True
        """

        # Wait for any ongoing environment operations
        self._environment_engine.await_environment_command()

        library_name = os.path.basename(path)

        path = util.convert_to_abs(path, self._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "wheel",
            "packageSource": "WHEEL",
        }

        library_rest = self._library_api.install(
            library_name, self.python_version, library_spec
        )

        if await_installation:
            return self._environment_engine.await_library_command(library_name)

        return library_rest

    def install_requirements(self, path, await_installation=True):
        """Install libraries specified in a requirements.txt file

        ```python

        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        requirements_path = ds_api.upload("requirements.txt", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment()
        env.install_requirements(requirements_path)

        ```

        # Arguments
            path: str. The path on Hopsworks where the requirements.txt file is located
            await_installation: bool. If True the method returns only when the installation is finished. Default True
        """

        # Wait for any ongoing environment operations
        self._environment_engine.await_environment_command()

        library_name = os.path.basename(path)

        path = util.convert_to_abs(path, self._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "requirements_txt",
            "packageSource": "REQUIREMENTS_TXT",
        }

        library_rest = self._library_api.install(
            library_name, self.python_version, library_spec
        )

        if await_installation:
            return self._environment_engine.await_library_command(library_name)

        return library_rest

    def delete(self):
        """Delete the environment
        !!! danger "Potentially dangerous operation"
            This operation deletes the python environment.
        # Raises
            `RestAPIError`.
        """
        self._environment_api._delete(self.python_version)

    def __repr__(self):
        return f"Environment({self._python_version!r})"
