#!/usr/bin/env python

"""Kafka Utility Belt.

This script contains a set of utility functions required for running the Kafka platform on Docker.

The script supports following commands:

1. kafka-ready : Ensures a Kafka cluster is ready to accept client requests.
2. zookeeper-ready: Ensures that a Zookeeper ensemble is ready to accept client requests.
3. wait-for-service: Ensures that a specific service is ready to accept client requests on HTTP.
4. listeners: Derives the listeners property from advertised.listeners.
5. kafka-topic: Ensure that topic exists and is vaild.

These commands log any output to stderr and returns with exitcode 0 if successful, 1 otherwise.

"""
from __future__ import print_function
import os
import sys
import socket
import time
import re
import subprocess

CLASSPATH = os.environ.get("KUB_CLASSPATH", '"/usr/share/kub/*"')


def wait_for_service(host, port, timeout):
    """Waits for a service to start listening on a port.

    Args:
        host: Hostname where the service is hosted.
        port: Port where the service is expected to bind.
        timeout: Time in secs to wait for the service to be available.

    Returns:
        False, if the timeout expires and the service is unreachable, True otherwise.

    """
    start = time.time()
    while True:
        try:
            s = socket.create_connection((host, int(port)), float(timeout))
            s.close()
            return True
        except socket.error:
            pass

        time.sleep(1)
        if time.time() - start > timeout:
            return False


def check_zookeeper_ready(connect_string, timeout):
    """Waits for a Zookeeper ensemble be ready. This commands uses the Java
       docker-utils library to get the Zookeeper status.
       This command supports a secure Zookeeper cluster. It expects the KAFKA_OPTS
       enviornment variable to contain the JAAS configuration.

    Args:
        connect_string: Zookeeper connection string (host:port, ....)
        timeout: Time in secs to wait for the Zookeeper to be available.

    Returns:
        False, if the timeout expires and Zookeeper is unreachable, True otherwise.

    """
    cmd_template = """
             java {jvm_opts} \
                 -cp {classpath} \
                 de.mbe1224.utils.cli.ZooKeeperReadyCommand \
                 {connect_string} \
                 {timeout_in_ms}"""

    # This is to ensure that we include KAFKA_OPTS only if the jaas.conf has
    # entries for zookeeper. If you enable SASL, it is recommended that you
    # should enable it for all the components. This is an option if SASL
    # cannot be enabled on Zookeeper.
    jvm_opts = ""
    is_zk_sasl_enabled = os.environ.get("ZOOKEEPER_SASL_ENABLED") or ""

    if (not is_zk_sasl_enabled.upper() == "FALSE") and os.environ.get("KAFKA_OPTS"):
        jvm_opts = os.environ.get("KAFKA_OPTS")

    cmd = cmd_template.format(
        classpath=CLASSPATH,
        jvm_opts=jvm_opts or "",
        connect_string=connect_string,
        timeout_in_ms=timeout * 1000)

    return subprocess.call(cmd, shell=True) == 0


def check_kafka_ready(expected_brokers, timeout, config, bootstrap_broker_list=None, zookeeper_connect=None, security_protocol=None):
    """Waits for a Kafka cluster to be ready and have at least the
       expected_brokers to present. This commands uses the Java docker-utils
       library to get the Kafka status.

       This command supports a secure Kafka cluster. If SSL is enabled, it
       expects the client_properties file to have the relevant SSL properties.
       If SASL is enabled, the command expects the JAAS config to be present in the
       KAFKA_OPTS environment variable and the SASL properties to present in the
       client_properties file.


    Args:
        expected_brokers: expected number of brokers in the cluster.
        timeout: Time in secs to wait for the Zookeeper to be available.
        config: properties file with client config for SSL and SASL.
        security_protocol: Security protocol to use.
        bootstrap_broker_list: Kafka bootstrap broker list string (host:port, ....)
        zookeeper_connect: Zookeeper connect string.

    Returns:
        False, if the timeout expires and Kafka cluster is unreachable, True otherwise.

    """
    cmd_template = """
             java {jvm_opts} \
                 -cp {classpath} \
                 de.mbe1224.utils.cli.KafkaReadyCommand \
                 {expected_brokers} \
                 {timeout_in_ms}"""

    cmd = cmd_template.format(
        classpath=CLASSPATH,
        jvm_opts=os.environ.get("KAFKA_OPTS") or "",
        bootstrap_broker_list=bootstrap_broker_list,
        expected_brokers=expected_brokers,
        timeout_in_ms=timeout * 1000)

    if config:
        cmd = "{cmd} --config {config_path}".format(cmd=cmd, config_path=config)

    if security_protocol:
        cmd = "{cmd} --security-protocol {protocol}".format(cmd=cmd, protocol=security_protocol)

    if bootstrap_broker_list:
        cmd = "{cmd} -b {broker_list}".format(cmd=cmd, broker_list=bootstrap_broker_list)
    else:
        cmd = "{cmd} -z {zookeeper_connect}".format(cmd=cmd, zookeeper_connect=zookeeper_connect)

    exit_code = subprocess.call(cmd, shell=True)

    if exit_code == 0:
        return True
    else:
        return False


def get_kafka_listeners(advertised_listeners):
    """Derives listeners property from advertised.listeners. It just converts the
       hostname to 0.0.0.0 so that Kafka process listens to all the interfaces.

       For example, if
            advertised_listeners = PLAINTEXT://foo:9999,SSL://bar:9098, SASL_SSL://10.0.4.5:7888
            then, the function will return
            PLAINTEXT://0.0.0.0:9999,SSL://0.0.0.0:9098, SASL_SSL://0.0.0.0:7888

    Args:
        advertised_listeners: advertised.listeners string.

    Returns:
        listeners string.

    """
    host = re.compile(r'://(.*?):', re.UNICODE)
    return host.sub(r'://0.0.0.0:', advertised_listeners)


def ensure_topic(config, file, timeout, create_if_not_exists):
    """Ensures that the topic in the file exists on the cluster and has valid config.


    Args:
        config: client config (properties file).
        timeout: Time in secs for all operations.
        file: YAML file with topic config.
        create_if_not_exists: Creates topics if they dont exist.

    Returns:
        False, if the timeout expires and Kafka cluster is unreachable, True otherwise.

    """
    cmd_template = """
             java {jvm_opts} \
                 -cp {classpath} \
                 de.mbe1224.utils.cli.KafkaTopicCommand \
                 --config {config} \
                 --file {file} \
                 --create-if-not-exists {create_if_not_exists} \
                 --timeout {timeout_in_ms}"""

    cmd = cmd_template.format(
        classpath=CLASSPATH,
        jvm_opts=os.environ.get("KAFKA_OPTS") or "",
        config=config,
        file=file,
        timeout_in_ms=timeout * 1000,
        create_if_not_exists=create_if_not_exists)

    exit_code = subprocess.call(cmd, shell=True)

    if exit_code == 0:
        return True
    else:
        return False


def main():
    import argparse
    root = argparse.ArgumentParser(description='Kafka Utility Belt.')

    actions = root.add_subparsers(help='Actions', dest='action')

    zk = actions.add_parser('zookeeper-ready', description='Check if ZK is ready.')
    zk.add_argument('connect_string', help='Zookeeper connect string.')
    zk.add_argument('timeout', help='Time in secs to wait for service to be ready.', type=int)

    kafka = actions.add_parser('kafka-ready', description='Check if Kafka is ready.')
    kafka.add_argument('expected_brokers', help='Minimum number of brokers to wait for', type=int)
    kafka.add_argument('timeout', help='Time in secs to wait for service to be ready.', type=int)
    kafka_or_zk = kafka.add_mutually_exclusive_group(required=True)
    kafka_or_zk.add_argument('-b', '--bootstrap_broker_list', help='List of bootstrap brokers.')
    kafka_or_zk.add_argument('-z', '--zookeeper_connect', help='Zookeeper connect string.')
    kafka.add_argument('-c', '--config', help='Path to config properties file (required when security is enabled).')
    kafka.add_argument('-s', '--security-protocol', help='Security protocol to use when multiple listeners are enabled.')

    ws = actions.add_parser('wait-for-service', description='Ensures that a specific service is ready to accept client requests on HTTP.')
    ws.add_argument('host', help='Hostname.')
    ws.add_argument('port', help='Port.')
    ws.add_argument('timeout', help='Time in secs to wait for service to be ready.', type=int)

    config = actions.add_parser('listeners', description='Get listeners value from advertised.listeners. Replaces host to 0.0.0.0')
    config.add_argument('advertised_listeners', help='advertised.listeners string.')

    te = actions.add_parser('kafka-topic', description='Ensure that topic exists and is valid.')
    te.add_argument('config', help='client config (properties file).')
    te.add_argument('file', help='YAML file with topic config.')
    te.add_argument('timeout', help='Time in secs for all operations.', type=int)
    te.add_argument('--create_if_not_exists', help='Create topics if they do not yet exist.', action='store_true')

    if len(sys.argv) < 2:
        root.print_help()
        sys.exit(1)

    args = root.parse_args()

    success = False

    if args.action == "zookeeper-ready":
        success = check_zookeeper_ready(args.connect_string, int(args.timeout))
    elif args.action == "kafka-ready":
        success = check_kafka_ready(int(args.expected_brokers), int(args.timeout), args.config, args.bootstrap_broker_list, args.zookeeper_connect, args.security_protocol)
    elif args.action == "wait-for-service":
        success = wait_for_service(args.host, args.port, int(args.timeout))
    elif args.action == "kafka-topic":
        success = ensure_topic(args.config, args.file, int(args.timeout), args.create_if_not_exists)
    elif args.action == "listeners":
        listeners = get_kafka_listeners(args.advertised_listeners)
        if listeners:
            # Print the output to stdout. Don't delete this, this is not for debugging.
            print(listeners)
            success = True

    if success:
        sys.exit(0)
    else:
        sys.exit(1)
