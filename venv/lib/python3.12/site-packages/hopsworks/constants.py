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


class JOBS:
    SUCCESS_STATES = ["FINISHED", "SUCCEEDED"]
    ERROR_STATES = [
        "FAILED",
        "KILLED",
        "FRAMEWORK_FAILURE",
        "APP_MASTER_START_FAILED",
        "INITIALIZATION_FAILED",
    ]


class GIT:
    SUCCESS_STATES = ["SUCCESS"]
    ERROR_STATES = ["FAILED", "KILLED", "INITIALIZATION_FAILED", "TIMEDOUT"]


class SERVICES:
    LIST = ["JOBS", "KAFKA", "JUPYTER", "HIVE", "SERVING", "FEATURESTORE", "AIRFLOW"]


class OPENSEARCH_CONFIG:
    SSL_CONFIG = "es.net.ssl"
    NODES_WAN_ONLY = "es.nodes.wan.only"
    NODES = "es.nodes"
    SSL_KEYSTORE_LOCATION = "es.net.ssl.keystore.location"
    SSL_KEYSTORE_PASSWORD = "es.net.ssl.keystore.pass"
    SSL_TRUSTSTORE_LOCATION = "es.net.ssl.truststore.location"
    SSL_TRUSTSTORE_PASSWORD = "es.net.ssl.truststore.pass"
    HTTP_AUTHORIZATION = "es.net.http.header.Authorization"
    INDEX = "es.resource"
    HOSTS = "hosts"
    HTTP_COMPRESS = "http_compress"
    HEADERS = "headers"
    USE_SSL = "use_ssl"
    VERIFY_CERTS = "verify_certs"
    SSL_ASSERT_HOSTNAME = "ssl_assert_hostname"
    CA_CERTS = "ca_certs"


class KAFKA_SSL_CONFIG:
    """
    Kafka SSL constant strings for configuration
    """

    SSL = "SSL"
    SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location"
    SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file. "
    SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password"
    SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. If a password is not set access to the truststore is still available, but integrity checking is disabled."
    SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location"
    SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password"
    SSL_KEY_PASSWORD_CONFIG = "ssl.key.password"
    SECURITY_PROTOCOL_CONFIG = "security.protocol"
    SSL_CERTIFICATE_LOCATION_CONFIG = "ssl.certificate.location"
    SSL_CA_LOCATION_CONFIG = "ssl.ca.location"
    SSL_PRIVATE_KEY_LOCATION_CONFIG = "ssl.key.location"
    SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = (
        "ssl.endpoint.identification.algorithm"
    )


class KAFKA_PRODUCER_CONFIG:
    """
    Constant strings for Kafka producers
    """

    BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
    KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
    VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"


class KAFKA_CONSUMER_CONFIG:
    """
    Constant strings for Kafka consumers
    """

    GROUP_ID_CONFIG = "group.id"
    CLIENT_ID_CONFIG = "client.id"
    ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
    AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms"
    SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms"
    KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer"
    VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer"
    AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset"
    ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
    KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer"
    VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer"


class ENV_VARS:
    """
    Constant strings for environment variables
    """

    KAFKA_BROKERS = "KAFKA_BROKERS"
    ELASTIC_ENDPOINT_ENV_VAR = "ELASTIC_ENDPOINT"


class SSL_CONFIG:
    """
    General SSL configuration constants for Hops-TLS
    """

    KEYSTORE_SUFFIX = "__kstore.jks"
    TRUSTSTORE_SUFFIX = "__tstore.jks"
    PASSWORD_SUFFIX = "__cert.key"

    K_CERTIFICATE_CONFIG = "k_certificate"
    T_CERTIFICATE_CONFIG = "t_certificate"
    PEM_CLIENT_CERTIFICATE_CONFIG = "client.pem"
    PEM_CLIENT_KEY_CONFIG = "client_key.pem"
    PEM_CA_CHAIN_CERTIFICATE_CONFIG = "ca_chain.pem"
    DOMAIN_CA_TRUSTSTORE = "domain_ca_truststore"
    CRYPTO_MATERIAL_PASSWORD = "material_passwd"
    PEM_CA_ROOT_CERT = "/srv/hops/kagent/host-certs/hops_root_ca.pem"
    SSL_ENABLED = "ipc.server.ssl.enabled"


class HOSTS:
    APP_HOST = "c.app.hopsworks.ai"
