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

from hopsworks import client, project, constants
import json
from hopsworks.client.exceptions import RestAPIError


class ProjectApi:
    def _exists(self, name: str):
        """Check if a project exists.

        # Arguments
            name: Name of the project.
        # Returns
            `bool`: True if project exists, otherwise False
        # Raises
            `RestAPIError`: If unable to check the existence of the project
        """
        try:
            self._get_project(name)
            return True
        except RestAPIError:
            return False

    def _get_projects(self):
        """Get all projects accessible by the user.

        # Returns
            `List[Project]`: List of Project objects
        # Raises
            `RestAPIError`: If unable to get the projects
        """
        _client = client.get_instance()
        path_params = [
            "project",
        ]
        project_team_json = _client._send_request("GET", path_params)
        projects = []
        for project_team in project_team_json:
            projects.append(self._get_project(project_team["project"]["name"]))
        return projects

    def _get_project(self, name: str):
        """Get a project.

        # Arguments
            name: Name of the project.
        # Returns
            `Project`: The Project object
        # Raises
            `RestAPIError`: If unable to get the project
        """
        _client = client.get_instance()
        path_params = [
            "project",
            "getProjectInfo",
            name,
        ]
        project_json = _client._send_request("GET", path_params)
        return project.Project.from_response_json(project_json)

    def _create_project(
        self, name: str, description: str = None, feature_store_topic: str = None
    ):
        """Create a new project.

        # Arguments
            name: Name of the project.
            description: Description of the project.
            feature_store_topic: Feature store topic name.
        # Returns
            `Project`: The Project object
        # Raises
            `RestAPIError`: If unable to create the project
        """
        _client = client.get_instance()

        path_params = ["project"]
        query_params = {"projectName": name}
        headers = {"content-type": "application/json"}

        data = {
            "projectName": name,
            "services": constants.SERVICES.LIST,
            "description": description,
            "featureStoreTopic": feature_store_topic,
        }
        _client._send_request(
            "POST",
            path_params,
            headers=headers,
            query_params=query_params,
            data=json.dumps(data),
        )

        # The return of the project creation is not a ProjectDTO, so get the correct object after creation
        project = self._get_project(name)
        print("Project created successfully, explore it at " + project.get_url())
        return project
