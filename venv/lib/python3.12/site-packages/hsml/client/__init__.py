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

from hsml.connection import CONNECTION_SAAS_HOSTNAME

from hsml.client.hopsworks import base as hw_base
from hsml.client.hopsworks import internal as hw_internal
from hsml.client.hopsworks import external as hw_external

from hsml.client.istio import base as ist_base
from hsml.client.istio import internal as ist_internal
from hsml.client.istio import external as ist_external


_client_type = None
_saas_connection = None

_hopsworks_client = None
_istio_client = None

_kserve_installed = None
_serving_resource_limits = None
_serving_num_instances_limits = None
_knative_domain = None


def init(
    client_type,
    host=None,
    port=None,
    project=None,
    hostname_verification=None,
    trust_store_path=None,
    api_key_file=None,
    api_key_value=None,
):
    global _client_type
    _client_type = client_type

    global _saas_connection
    _saas_connection = host == CONNECTION_SAAS_HOSTNAME

    global _hopsworks_client
    if not _hopsworks_client:
        if client_type == "internal":
            _hopsworks_client = hw_internal.Client()
        elif client_type == "external":
            _hopsworks_client = hw_external.Client(
                host,
                port,
                project,
                hostname_verification,
                trust_store_path,
                api_key_file,
                api_key_value,
            )


def get_instance() -> hw_base.Client:
    global _hopsworks_client
    if _hopsworks_client:
        return _hopsworks_client
    raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")


def set_istio_client(host, port, project=None, api_key_value=None):
    global _client_type, _istio_client

    if not _istio_client:
        if _client_type == "internal":
            _istio_client = ist_internal.Client(host, port)
        elif _client_type == "external":
            _istio_client = ist_external.Client(host, port, project, api_key_value)


def get_istio_instance() -> ist_base.Client:
    global _istio_client
    return _istio_client


def get_client_type() -> str:
    global _client_type
    return _client_type


def is_saas_connection() -> bool:
    global _saas_connection
    return _saas_connection


def set_kserve_installed(kserve_installed):
    global _kserve_installed
    _kserve_installed = kserve_installed


def is_kserve_installed() -> bool:
    global _kserve_installed
    return _kserve_installed


def set_serving_resource_limits(max_resources):
    global _serving_resource_limits
    _serving_resource_limits = max_resources


def get_serving_resource_limits():
    global _serving_resource_limits
    return _serving_resource_limits


def set_serving_num_instances_limits(num_instances_range):
    global _serving_num_instances_limits
    _serving_num_instances_limits = num_instances_range


def get_serving_num_instances_limits():
    global _serving_num_instances_limits
    return _serving_num_instances_limits


def get_knative_domain():
    global _knative_domain
    return _knative_domain


def set_knative_domain(knative_domain):
    global _knative_domain
    _knative_domain = knative_domain


def stop():
    global _hopsworks_client, _istio_client
    _hopsworks_client._close()
    _istio_client._close()
    _hopsworks_client = _istio_client = None
