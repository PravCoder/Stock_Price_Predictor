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

import json
import tempfile
import uuid
import time
import os
import importlib
from tqdm.auto import tqdm

from hsml.client.exceptions import RestAPIError, ModelRegistryException
from hsml import client, util, constants
from hsml.core import model_api, dataset_api

from hsml.engine import local_engine, hopsworks_engine


class ModelEngine:
    def __init__(self):
        self._model_api = model_api.ModelApi()
        self._dataset_api = dataset_api.DatasetApi()

        pydoop_spec = importlib.util.find_spec("pydoop")
        if pydoop_spec is None:
            self._engine = local_engine.LocalEngine()
        else:
            self._engine = hopsworks_engine.HopsworksEngine()

    def _poll_model_available(self, model_instance, await_registration):
        if await_registration > 0:
            model_registry_id = model_instance.model_registry_id
            sleep_seconds = 5
            for i in range(int(await_registration / sleep_seconds)):
                try:
                    time.sleep(sleep_seconds)
                    model_meta = self._model_api.get(
                        model_instance.name,
                        model_instance.version,
                        model_registry_id,
                        model_instance.shared_registry_project_name,
                    )
                    if model_meta is not None:
                        return model_meta
                except RestAPIError as e:
                    if e.response.status_code != 404:
                        raise e
            print(
                "Model not available during polling, set a higher value for await_registration to wait longer."
            )

    def _upload_additional_resources(self, model_instance):
        if model_instance._input_example is not None:
            input_example_path = os.path.join(os.getcwd(), "input_example.json")
            input_example = util.input_example_to_json(model_instance._input_example)

            with open(input_example_path, "w+") as out:
                json.dump(input_example, out, cls=util.NumpyEncoder)

            self._engine.upload(input_example_path, model_instance.version_path)
            os.remove(input_example_path)
            model_instance.input_example = None
        if model_instance._model_schema is not None:
            model_schema_path = os.path.join(os.getcwd(), "model_schema.json")
            model_schema = model_instance._model_schema

            with open(model_schema_path, "w+") as out:
                out.write(model_schema.json())

            self._engine.upload(model_schema_path, model_instance.version_path)
            os.remove(model_schema_path)
            model_instance.model_schema = None
        return model_instance

    def _copy_or_move_hopsfs_model_item(
        self, item_attr, to_model_version_path, keep_original_files
    ):
        """Copy or move model item from a hdfs path to the model version folder in the Models dataset. It works with files and folders."""
        path = item_attr["path"]
        to_hdfs_path = os.path.join(to_model_version_path, os.path.basename(path))
        if keep_original_files:
            self._engine.copy(path, to_hdfs_path)
        else:
            self._engine.move(path, to_hdfs_path)

    def _copy_or_move_hopsfs_model(
        self,
        from_hdfs_model_path,
        to_model_version_path,
        keep_original_files,
        update_upload_progress,
    ):
        """Copy or move model files from a hdfs path to the model version folder in the Models dataset."""
        # Strip hdfs prefix
        if from_hdfs_model_path.startswith("hdfs:/"):
            projects_index = from_hdfs_model_path.find("/Projects", 0)
            from_hdfs_model_path = from_hdfs_model_path[projects_index:]

        n_dirs, n_files = 0, 0

        model_path_resp = self._dataset_api.get(from_hdfs_model_path)
        model_path_attr = model_path_resp["attributes"]
        if (
            "datasetType" in model_path_resp
            and model_path_resp["datasetType"] == "DATASET"
        ):  # This is needed to avoid a user exporting for example "Resources" from wiping the dataset
            raise AssertionError(
                "It is disallowed to export a root dataset path."
                " Move the model to a sub-folder and try again."
            )
        elif model_path_attr.get("dir", False):
            # if path is a directory, iterate of the directory content
            for entry in self._dataset_api.list(
                from_hdfs_model_path, sort_by="NAME:desc"
            )["items"]:
                path_attr = entry["attributes"]
                self._copy_or_move_hopsfs_model_item(
                    path_attr, to_model_version_path, keep_original_files
                )
                if path_attr.get("dir", False):
                    n_dirs += 1
                else:
                    n_files += 1
                update_upload_progress(n_dirs=n_dirs, n_files=n_files)
        else:
            # if path is a file, copy/move it
            self._copy_or_move_hopsfs_model_item(
                model_path_attr, to_model_version_path, keep_original_files
            )
            n_files += 1
            update_upload_progress(n_dirs=n_dirs, n_files=n_files)

    def _download_model_from_hopsfs_recursive(
        self,
        from_hdfs_model_path: str,
        to_local_path: str,
        update_download_progress,
        n_dirs,
        n_files,
    ):
        """Download model files from a model path in hdfs, recursively"""

        for entry in self._dataset_api.list(from_hdfs_model_path, sort_by="NAME:desc")[
            "items"
        ]:
            path_attr = entry["attributes"]
            path = path_attr["path"]
            basename = os.path.basename(path)

            if path_attr.get("dir", False):
                # otherwise, make a recursive call for the folder
                if basename == "Artifacts":
                    continue  # skip Artifacts subfolder
                local_folder_path = os.path.join(to_local_path, basename)
                os.mkdir(local_folder_path)
                n_dirs, n_files = self._download_model_from_hopsfs_recursive(
                    from_hdfs_model_path=path,
                    to_local_path=local_folder_path,
                    update_download_progress=update_download_progress,
                    n_dirs=n_dirs,
                    n_files=n_files,
                )
                n_dirs += 1
                update_download_progress(n_dirs=n_dirs, n_files=n_files)
            else:
                # if it's a file, download it
                local_file_path = os.path.join(to_local_path, basename)
                self._engine.download(path, local_file_path)
                n_files += 1
                update_download_progress(n_dirs=n_dirs, n_files=n_files)

        return n_dirs, n_files

    def _download_model_from_hopsfs(
        self, from_hdfs_model_path: str, to_local_path: str, update_download_progress
    ):
        """Download model files from a model path in hdfs."""

        n_dirs, n_files = self._download_model_from_hopsfs_recursive(
            from_hdfs_model_path=from_hdfs_model_path,
            to_local_path=to_local_path,
            update_download_progress=update_download_progress,
            n_dirs=0,
            n_files=0,
        )
        update_download_progress(n_dirs=n_dirs, n_files=n_files, done=True)

    def _upload_local_model(
        self,
        from_local_model_path,
        to_model_version_path,
        update_upload_progress,
        upload_configuration=None,
    ):
        """Copy or upload model files from a local path to the model version folder in the Models dataset."""
        n_dirs, n_files = 0, 0
        if os.path.isdir(from_local_model_path):
            # if path is a dir, upload files and folders iteratively
            for root, dirs, files in os.walk(from_local_model_path):
                # os.walk(local_model_path), where local_model_path is expected to be an absolute path
                # - root is the absolute path of the directory being walked
                # - dirs is the list of directory names present in the root dir
                # - files is the list of file names present in the root dir
                # we need to replace the local path prefix with the hdfs path prefix (i.e., /srv/hops/....../root with /Projects/.../)
                remote_base_path = root.replace(
                    from_local_model_path, to_model_version_path
                )
                for d_name in dirs:
                    self._engine.mkdir(remote_base_path + "/" + d_name)
                    n_dirs += 1
                    update_upload_progress(n_dirs, n_files)
                for f_name in files:
                    self._engine.upload(
                        root + "/" + f_name,
                        remote_base_path,
                        upload_configuration=upload_configuration,
                    )
                    n_files += 1
                    update_upload_progress(n_dirs, n_files)
        else:
            # if path is a file, upload file
            self._engine.upload(
                from_local_model_path,
                to_model_version_path,
                upload_configuration=upload_configuration,
            )
            n_files += 1
            update_upload_progress(n_dirs, n_files)

    def _save_model_from_local_or_hopsfs_mount(
        self,
        model_instance,
        model_path,
        keep_original_files,
        update_upload_progress,
        upload_configuration=None,
    ):
        """Save model files from a local path. The local path can be on hopsfs mount"""
        # check hopsfs mount
        if model_path.startswith(constants.MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX):
            self._copy_or_move_hopsfs_model(
                from_hdfs_model_path=model_path.replace(
                    constants.MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX, ""
                ),
                to_model_version_path=model_instance.version_path,
                keep_original_files=keep_original_files,
                update_upload_progress=update_upload_progress,
            )
        else:
            self._upload_local_model(
                from_local_model_path=model_path,
                to_model_version_path=model_instance.version_path,
                update_upload_progress=update_upload_progress,
                upload_configuration=upload_configuration,
            )

    def _set_model_version(
        self, model_instance, dataset_models_root_path, dataset_model_path
    ):
        # Set model version if not defined
        if model_instance._version is None:
            current_highest_version = 0
            for item in self._dataset_api.list(dataset_model_path, sort_by="NAME:desc")[
                "items"
            ]:
                _, file_name = os.path.split(item["attributes"]["path"])
                try:
                    try:
                        current_version = int(file_name)
                    except ValueError:
                        continue
                    if current_version > current_highest_version:
                        current_highest_version = current_version
                except RestAPIError:
                    pass
            model_instance._version = current_highest_version + 1

        elif self._dataset_api.path_exists(
            dataset_models_root_path
            + "/"
            + model_instance._name
            + "/"
            + str(model_instance._version)
        ):
            raise ModelRegistryException(
                "Model with name {} and version {} already exists".format(
                    model_instance._name, model_instance._version
                )
            )
        return model_instance

    def _build_resource_path(self, model_instance, artifact):
        artifact_path = "{}/{}".format(model_instance.version_path, artifact)
        return artifact_path

    def save(
        self,
        model_instance,
        model_path,
        await_registration=480,
        keep_original_files=False,
        upload_configuration=None,
    ):
        _client = client.get_instance()

        is_shared_registry = model_instance.shared_registry_project_name is not None

        if is_shared_registry:
            dataset_models_root_path = "{}::{}".format(
                model_instance.shared_registry_project_name,
                constants.MODEL_SERVING.MODELS_DATASET,
            )
            model_instance._project_name = model_instance.shared_registry_project_name
        else:
            dataset_models_root_path = constants.MODEL_SERVING.MODELS_DATASET
            model_instance._project_name = _client._project_name

        util.validate_metrics(model_instance.training_metrics)

        if not self._dataset_api.path_exists(dataset_models_root_path):
            raise AssertionError(
                "{} dataset does not exist in this project. Please enable the Serving service or create it manually.".format(
                    dataset_models_root_path
                )
            )

        # Create /Models/{model_instance._name} folder
        dataset_model_name_path = dataset_models_root_path + "/" + model_instance._name
        if not self._dataset_api.path_exists(dataset_model_name_path):
            self._engine.mkdir(dataset_model_name_path)

        model_instance = self._set_model_version(
            model_instance, dataset_models_root_path, dataset_model_name_path
        )

        # Attach model summary xattr to /Models/{model_instance._name}/{model_instance._version}
        model_query_params = {}

        if "ML_ID" in os.environ:
            model_instance._experiment_id = os.environ["ML_ID"]

        model_instance._experiment_project_name = _client._project_name

        if "HOPSWORKS_JOB_NAME" in os.environ:
            model_query_params["jobName"] = os.environ["HOPSWORKS_JOB_NAME"]
        elif "HOPSWORKS_KERNEL_ID" in os.environ:
            model_query_params["kernelId"] = os.environ["HOPSWORKS_KERNEL_ID"]

        pbar = tqdm(
            [
                {"id": 0, "desc": "Creating model folder"},
                {"id": 1, "desc": "Uploading model files"},
                {"id": 2, "desc": "Uploading input_example and model_schema"},
                {"id": 3, "desc": "Registering model"},
                {"id": 4, "desc": "Waiting for model registration"},
                {"id": 5, "desc": "Model export complete"},
            ]
        )

        for step in pbar:
            try:
                pbar.set_description("%s" % step["desc"])
                if step["id"] == 0:
                    # Create folders
                    self._engine.mkdir(model_instance.version_path)
                if step["id"] == 1:

                    def update_upload_progress(n_dirs=0, n_files=0):
                        pbar.set_description(
                            "%s (%s dirs, %s files)" % (step["desc"], n_dirs, n_files)
                        )

                    update_upload_progress(n_dirs=0, n_files=0)

                    # Upload Model files from local path to /Models/{model_instance._name}/{model_instance._version}
                    # check local absolute
                    if os.path.isabs(model_path) and os.path.exists(model_path):
                        self._save_model_from_local_or_hopsfs_mount(
                            model_instance=model_instance,
                            model_path=model_path,
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                            upload_configuration=upload_configuration,
                        )
                    # check local relative
                    elif os.path.exists(
                        os.path.join(os.getcwd(), model_path)
                    ):  # check local relative
                        self._save_model_from_local_or_hopsfs_mount(
                            model_instance=model_instance,
                            model_path=os.path.join(os.getcwd(), model_path),
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                            upload_configuration=upload_configuration,
                        )
                    # check project relative
                    elif self._dataset_api.path_exists(
                        model_path
                    ):  # check hdfs relative and absolute
                        self._copy_or_move_hopsfs_model(
                            from_hdfs_model_path=model_path,
                            to_model_version_path=model_instance.version_path,
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                        )
                    else:
                        raise IOError(
                            "Could not find path {} in the local filesystem or in Hopsworks File System".format(
                                model_path
                            )
                        )
                if step["id"] == 2:
                    model_instance = self._upload_additional_resources(model_instance)
                if step["id"] == 3:
                    model_instance = self._model_api.put(
                        model_instance, model_query_params
                    )
                if step["id"] == 4:
                    model_instance = self._poll_model_available(
                        model_instance, await_registration
                    )
                if step["id"] == 5:
                    pass
            except BaseException as be:
                self._dataset_api.rm(model_instance.version_path)
                raise be

        print("Model created, explore it at " + model_instance.get_url())

        return model_instance

    def download(self, model_instance):
        model_name_path = os.path.join(
            tempfile.gettempdir(), str(uuid.uuid4()), model_instance._name
        )
        model_version_path = model_name_path + "/" + str(model_instance._version)
        os.makedirs(model_version_path)

        def update_download_progress(n_dirs, n_files, done=False):
            print(
                "Downloading model artifact (%s dirs, %s files)... %s"
                % (n_dirs, n_files, "DONE" if done else ""),
                end="\r",
            )

        try:
            from_hdfs_model_path = model_instance.version_path
            if from_hdfs_model_path.startswith("hdfs:/"):
                projects_index = from_hdfs_model_path.find("/Projects", 0)
                from_hdfs_model_path = from_hdfs_model_path[projects_index:]

            self._download_model_from_hopsfs(
                from_hdfs_model_path=from_hdfs_model_path,
                to_local_path=model_version_path,
                update_download_progress=update_download_progress,
            )
        except BaseException as be:
            raise be

        return model_version_path

    def read_file(self, model_instance, resource):
        hdfs_resource_path = self._build_resource_path(
            model_instance, os.path.basename(resource)
        )
        if self._dataset_api.path_exists(hdfs_resource_path):
            try:
                resource = os.path.basename(resource)
                tmp_dir = tempfile.TemporaryDirectory(dir=os.getcwd())
                local_resource_path = os.path.join(tmp_dir.name, resource)
                self._engine.download(
                    hdfs_resource_path,
                    local_resource_path,
                )
                with open(local_resource_path, "r") as f:
                    return f.read()
            finally:
                if tmp_dir is not None and os.path.exists(tmp_dir.name):
                    tmp_dir.cleanup()

    def read_json(self, model_instance, resource):
        hdfs_resource_path = self._build_resource_path(model_instance, resource)
        if self._dataset_api.path_exists(hdfs_resource_path):
            try:
                tmp_dir = tempfile.TemporaryDirectory(dir=os.getcwd())
                local_resource_path = os.path.join(tmp_dir.name, resource)
                self._engine.download(
                    hdfs_resource_path,
                    local_resource_path,
                )
                with open(local_resource_path, "rb") as f:
                    return json.loads(f.read())
            finally:
                if tmp_dir is not None and os.path.exists(tmp_dir.name):
                    tmp_dir.cleanup()

    def delete(self, model_instance):
        self._engine.delete(model_instance)

    def set_tag(self, model_instance, name, value):
        """Attach a name/value tag to a model."""
        self._model_api.set_tag(model_instance, name, value)

    def delete_tag(self, model_instance, name):
        """Remove a tag from a model."""
        self._model_api.delete_tag(model_instance, name)

    def get_tag(self, model_instance, name):
        """Get tag with a certain name."""
        return self._model_api.get_tags(model_instance, name)[name]

    def get_tags(self, model_instance):
        """Get all tags for a model."""
        return self._model_api.get_tags(model_instance)
