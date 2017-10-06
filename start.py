# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import re
import socket
import textwrap

from ambariclient.client import Ambari

from clusterdock.models import Cluster, client, Node
from clusterdock.utils import nested_get, wait_for_condition

logger = logging.getLogger('clusterdock.{}'.format(__name__))

DEFAULT_NAMESPACE = 'clusterdock'

AMBARI_AGENT_CONFIG_FILE_PATH = '/etc/ambari-agent/conf/ambari-agent.ini'
AMBARI_PORT = 8080


def main(args):
    image_prefix = '{}/{}/topology_hdp:hdp{}_ambari{}'.format(args.registry,
                                                              args.namespace or DEFAULT_NAMESPACE,
                                                              args.hdp_version,
                                                              args.ambari_version)
    primary_node_image = '{}_{}'.format(image_prefix, 'primary-node')
    secondary_node_image = '{}_{}'.format(image_prefix, 'secondary-node')

    primary_node = Node(hostname=args.primary_node[0], group='primary',
                        image=primary_node_image, ports=[{AMBARI_PORT: AMBARI_PORT}
                                                         if args.predictable
                                                         else AMBARI_PORT])

    secondary_nodes = [Node(hostname=hostname, group='secondary', image=secondary_node_image)
                       for hostname in args.secondary_nodes]

    cluster = Cluster(primary_node, *secondary_nodes)
    cluster.primary_node = primary_node
    cluster.secondary_nodes = secondary_nodes
    cluster.start(args.network)

    logger.debug('Starting PostgreSQL for Ambari server ...')
    primary_node.execute('service postgresql start', quiet=not args.verbose)
    _update_node_names(cluster, quiet=not args.verbose)

    # The HDP topology uses two pre-built images ('primary' and 'secondary'). If a cluster
    # larger than 2 nodes is started, some modifications need to be done.
    if len(secondary_nodes) > 1:
        _remove_files(nodes=secondary_nodes[1:],
                      files=['/hadoop/hdfs/current/*'])

    # logger.debug('Waiting for PostgreSQL to be started ...')
    # def condition(node):
    #     return node.execute('service postgresql status') == 0
    # def success(time):
    #     logger.debug('PostgreSQL became started after %s seconds.', time)
    # def failure(timeout):
    #     raise TimeoutError('Timed out after {} seconds waiting for '
    #                        'PostgreSQL to become started.'.format(timeout))
    # wait_for_condition(condition=condition, condition_args=[primary_node],
    #                    success=success, failure=failure)

    logger.info('Starting Ambari server ...')
    primary_node.execute('ambari-server start', quiet=not args.verbose)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    hostname = 'localhost' if client.info().get('Name') == 'moby' else socket.gethostname()
    port = cluster.primary_node.host_ports.get(AMBARI_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Ambari server is now reachable at %s', server_url)

    logger.info('Starting Ambari agents ...')
    for node in cluster:
        logger.debug('Starting Ambari agent on %s ...', node.fqdn)
        node.execute('ambari-agent start', quiet=not args.verbose)

    ambari = Ambari(server_url, username='admin', password='admin')
    for node in secondary_nodes[1:]:
        logger.info('Adding %s to cluster ...', node.fqdn)
        ambari.clusters('cluster').hosts.create(node.fqdn)
        for component in ambari.clusters('cluster').hosts(secondary_nodes[0].fqdn).components:
            logger.debug('Adding component (%s) to cluster on host (%s) ...',
                         component.component_name,
                         node.fqdn)
            host_components = ambari.clusters('cluster').hosts(node.fqdn).components
            host_components.create(component.component_name).wait()

        logger.debug('Installing all registered components on host (%s) ...',
                     node.fqdn)
        ambari.clusters('cluster').hosts(node.fqdn).components.install().wait()

    if not args.dont_start_cluster:
        logger.info('Starting cluster services ...')
        ambari.clusters('cluster').services.start().wait()


def _update_node_names(cluster, quiet):
    logger.info('Stopping Ambari server ...')
    command = cluster.primary_node.execute('ambari-server stop', quiet=quiet)
    if command.exit_code != 0:
        raise Exception('Ambari server returned non-zero exit code ({}) while '
                        'stopping. Full output:'
                        '\n{}'.format(command.exit_code,
                                      textwrap.indent(command.output,
                                                      prefix='    ')))

    logger.info('Stopping Ambari agents ...')
    commands = cluster.execute('ambari-agent stop', quiet=quiet)
    for node, command in commands.items():
        if command.exit_code != 0:
            raise Exception('Ambari agent on node ({}) returned non-zero exit code ({}) while '
                            'stopping. Full output:'
                            '\n{}'.format(node, command.exit_code,
                                          textwrap.indent(command.output, prefix='    ')))

    logger.debug('Creating host name changes JSON ...')
    host_name_changes = {'cluster': {'node-1.cluster': cluster.primary_node.fqdn,
                                     'node-2.cluster': cluster.secondary_nodes[0].fqdn}}
    cluster.primary_node.put_file('/root/host_name_changes.json', json.dumps(host_name_changes))
    command = ("/usr/jdk64/jdk1.8.0_112/bin/java -cp "
               "'/etc/ambari-server/conf:/usr/lib/ambari-server/*:"
               "/usr/share/java/postgresql-jdbc.jar' "
               "org.apache.ambari.server.update.HostUpdateHelper "
               "/root/host_name_changes.json > /var/log/ambari-server/ambari-server.out 2>&1")
    cluster.primary_node.execute(command)

    for node in cluster:
        ambari_agent_config = node.get_file(AMBARI_AGENT_CONFIG_FILE_PATH)

        logger.debug('Changing server hostname to %s ...', cluster.primary_node.fqdn)
        # ConfigObj.write returns a list of strings.
        node.put_file(AMBARI_AGENT_CONFIG_FILE_PATH,
                      re.sub(r'(hostname)=.*',
                             r'\1={}'.format(cluster.primary_node.fqdn),
                             ambari_agent_config))


def _remove_files(nodes, files):
    command = 'rm -rf {}'.format(' '.join(files))
    logger.info('Removing files (%s) from nodes (%s) ...',
                ', '.join(files),
                ', '.join(node.fqdn for node in nodes))
    for node in nodes:
        node.execute(command=command)
