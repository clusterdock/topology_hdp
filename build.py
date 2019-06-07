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

# useful build commands:
#   clusterdock build topology_hdp --operating-system centos6.8 --ambari-version 2.6.1.0 --hdp-version 2.6.4.0 --retain
#   clusterdock build topology_hdp --operating-system centos7.4 --ambari-version 2.7.3.0 --hdp-version 3.1.0.0 --retain

import json
import logging
import re
import socket
import time

import requests
from ambariclient.client import Ambari

import clusterdock.models as models
from clusterdock.config import defaults
from clusterdock.utils import join_url_parts, print_topology_meta, version_tuple, wait_for_condition

logger = logging.getLogger('clusterdock.{}'.format(__name__))

DEFAULT_AMBARI_PASSWORD = 'admin'
DEFAULT_AMBARI_USERNAME = 'admin'
DEFAULT_OPERATING_SYSTEM = 'centos7.4'

AMBARI_AGENT_CONFIG_FILE_PATH = '/etc/ambari-agent/conf/ambari-agent.ini'
AMBARI_PORT = 8080

# bare min and mandatory cluster config components
DEFAULT_BASE_HOST_GROUPS = [
    {'name': 'primary', 'cardinality': '1',
     'components': [{'name': 'NAMENODE'}, {'name': 'SECONDARY_NAMENODE'},
                    {'name': 'RESOURCEMANAGER'}, {'name': 'HISTORYSERVER'},
                    {'name': 'ZOOKEEPER_SERVER'}, {'name': 'APP_TIMELINE_SERVER'}]},
    {'name': 'secondary', 'cardinality': '1+',
     'components': [{'name': 'DATANODE'}, {'name': 'HDFS_CLIENT'},
                    {'name': 'NODEMANAGER'}, {'name': 'YARN_CLIENT'},
                    {'name': 'MAPREDUCE2_CLIENT'}, {'name': 'ZOOKEEPER_CLIENT'}]}]

EXTRA_HOST_GROUPS_2_0_13_0 = [
    {'components': [{'name': 'HIVE_SERVER'}, {'name': 'HBASE_MASTER'}, {'name': 'HIVE_METASTORE'},
                    {'name': 'HCAT'}, {'name': 'WEBHCAT_SERVER'}, {'name': 'MYSQL_SERVER'},
                    {'name': 'PIG'}, {'name': 'AMBARI_SERVER'}, {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'}]},
    {'components': [{'name': 'HCAT'}, {'name': 'PIG'}, {'name': 'HBASE_REGIONSERVER'},
                    {'name': 'HBASE_CLIENT'}, {'name': 'HIVE_CLIENT'}]}]

EXTRA_HOST_GROUPS_2_4_0_0 = [
    {'components': [{'name': 'HIVE_SERVER'}, {'name': 'HBASE_MASTER'}, {'name': 'HIVE_METASTORE'},
                    {'name': 'TEZ_CLIENT'}, {'name': 'HCAT'}, {'name': 'WEBHCAT_SERVER'},
                    {'name': 'KAFKA_BROKER'}, {'name': 'SLIDER'}, {'name': 'SPARK_CLIENT'},
                    {'name': 'MYSQL_SERVER'}, {'name': 'PIG'}, {'name': 'AMBARI_SERVER'},
                    {'name': 'HBASE_CLIENT'}, {'name': 'SPARK_JOBHISTORYSERVER'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_SERVER'},  {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR'},  {'name': 'INFRA_SOLR_CLIENT'}]},
    {'components': [{'name': 'SPARK_CLIENT'}, {'name': 'TEZ_CLIENT'}, {'name': 'HCAT'},
                    {'name': 'PIG'}, {'name': 'SLIDER'}, {'name': 'HBASE_REGIONSERVER'},
                    {'name': 'HBASE_CLIENT'}, {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR_CLIENT'}]}]

EXTRA_HOST_GROUPS_2_6_4_0 = [
    {'components': [{'name': 'HIVE_SERVER'}, {'name': 'SPARK_CLIENT'}, {'name': 'SPARK2_CLIENT'},
                    {'name': 'HBASE_MASTER'},
                    {'name': 'HIVE_METASTORE'}, {'name': 'TEZ_CLIENT'},
                    {'name': 'WEBHCAT_SERVER'}, {'name': 'HCAT'}, {'name': 'SLIDER'},
                    {'name': 'SPARK_JOBHISTORYSERVER'}, {'name': 'SPARK2_JOBHISTORYSERVER'},
                    {'name': 'KAFKA_BROKER'},
                    {'name': 'MYSQL_SERVER'}, {'name': 'PIG'}, {'name': 'AMBARI_SERVER'},
                    {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_SERVER'},  {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR'},  {'name': 'INFRA_SOLR_CLIENT'}]},
    {'components': [{'name': 'SPARK_CLIENT'}, {'name': 'SPARK2_CLIENT'}, {'name': 'TEZ_CLIENT'},
                    {'name': 'PIG'}, {'name': 'HCAT'},
                    {'name': 'HBASE_REGIONSERVER'}, {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'}, {'name': 'SLIDER'},
                    {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR_CLIENT'}]}]

EXTRA_HOST_GROUPS_3_1_0_0 = [
    {'components': [{'name': 'HIVE_SERVER'}, {'name': 'SPARK2_CLIENT'}, {'name': 'HBASE_MASTER'},
                    {'name': 'HIVE_METASTORE'}, {'name': 'TEZ_CLIENT'},
                    {'name': 'SPARK2_JOBHISTORYSERVER'},
                    {'name': 'KAFKA_BROKER'},
                    {'name': 'MYSQL_SERVER'}, {'name': 'PIG'}, {'name': 'AMBARI_SERVER'},
                    {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_SERVER'},  {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR'},  {'name': 'INFRA_SOLR_CLIENT'}]},
    {'components': [{'name': 'SPARK2_CLIENT'}, {'name': 'TEZ_CLIENT'},
                    {'name': 'PIG'},
                    {'name': 'HBASE_REGIONSERVER'}, {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR_CLIENT'}]}]

DEFAULT_EXTRA_HOST_GROUPS = [
    {'components': [{'name': 'HIVE_SERVER'}, {'name': 'SPARK2_CLIENT'}, {'name': 'HBASE_MASTER'},
                    {'name': 'HIVE_METASTORE'}, {'name': 'TEZ_CLIENT'}, {'name': 'HCAT'},
                    {'name': 'SPARK2_JOBHISTORYSERVER'}, {'name': 'WEBHCAT_SERVER'},
                    {'name': 'KAFKA_BROKER'}, {'name': 'SLIDER'}, {'name': 'SPARK_CLIENT'},
                    {'name': 'MYSQL_SERVER'}, {'name': 'PIG'}, {'name': 'AMBARI_SERVER'},
                    {'name': 'HBASE_CLIENT'}, {'name': 'SPARK_JOBHISTORYSERVER'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_SERVER'},  {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR'},  {'name': 'INFRA_SOLR_CLIENT'}]},
    {'components': [{'name': 'SPARK_CLIENT'}, {'name': 'SPARK2_CLIENT'}, {'name': 'TEZ_CLIENT'},
                    {'name': 'HCAT'}, {'name': 'PIG'}, {'name': 'SLIDER'},
                    {'name': 'HBASE_REGIONSERVER'}, {'name': 'HBASE_CLIENT'},
                    {'name': 'HIVE_CLIENT'},
                    {'name': 'ATLAS_CLIENT'},
                    {'name': 'INFRA_SOLR_CLIENT'}]}]

DEFAULT_CLUSTER_HOST_MAPPING = [{'name': 'primary', 'hosts': [{'fqdn': None}]},
                                {'name': 'secondary', 'hosts': [{'fqdn': None}]}]

# Ambari and HDP stack compatibility matrix https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/bk_support-matrices/content/ch_matrices-ambari.html#ambari_stack
# Ambari 4 digit version can be inferred from a link such as, if only found active: http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.6.1.0/ambari.repo
# HDP 4 digit version can be inferred from a link such as, if only found active: http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.6.4.0/hdp.repo

def main(args):
    quiet = not args.verbose
    print_topology_meta(args.topology)

    models.LOCALTIME_MOUNT = False
    models.PRIVILEGED_CONTAINER = True # 'privileged' containers are needed to have systemd work with no issues

    os_major_version = (args.operating_system or DEFAULT_OPERATING_SYSTEM)[6] # always assume 'centosX'
    image = '{}/topology_nodebase:{}'.format(defaults['DEFAULT_REPOSITORY'],
                                             args.operating_system or DEFAULT_OPERATING_SYSTEM)
    primary_node = models.Node(hostname='node-1', group='nodes', image=image, ports=[{AMBARI_PORT: AMBARI_PORT}])
    secondary_node = models.Node(hostname='node-2', group='nodes', image=image)
    cluster = models.Cluster(primary_node, secondary_node)
    cluster.start(args.network)

    hdp_version_tuple = version_tuple(args.hdp_version)
    stack_version = '{}.{}'.format(hdp_version_tuple[0], hdp_version_tuple[1])
    stack_version_tuple = (hdp_version_tuple[0], hdp_version_tuple[1])
    DEFAULT_CLUSTER_HOST_MAPPING[0]['hosts'][0]['fqdn'] = primary_node.fqdn
    DEFAULT_CLUSTER_HOST_MAPPING[1]['hosts'][0]['fqdn'] = secondary_node.fqdn

    host_groups = DEFAULT_BASE_HOST_GROUPS
    if not args.bare:
        if hdp_version_tuple <= (2, 0, 13, 0):
            host_groups[0]['components'].extend(EXTRA_HOST_GROUPS_2_0_13_0[0]['components'])
            host_groups[1]['components'].extend(EXTRA_HOST_GROUPS_2_0_13_0[1]['components'])
        elif hdp_version_tuple <= (2, 4, 0, 0):
            host_groups[0]['components'].extend(EXTRA_HOST_GROUPS_2_4_0_0[0]['components'])
            host_groups[1]['components'].extend(EXTRA_HOST_GROUPS_2_4_0_0[1]['components'])
        elif hdp_version_tuple <= (2, 6, 4, 0):
            host_groups[0]['components'].extend(EXTRA_HOST_GROUPS_2_6_4_0[0]['components'])
            host_groups[1]['components'].extend(EXTRA_HOST_GROUPS_2_6_4_0[1]['components'])
        elif hdp_version_tuple <= (3, 1, 0, 0):
            host_groups[0]['components'].extend(EXTRA_HOST_GROUPS_3_1_0_0[0]['components'])
            host_groups[1]['components'].extend(EXTRA_HOST_GROUPS_3_1_0_0[1]['components'])
        else:
            host_groups[0]['components'].extend(DEFAULT_EXTRA_HOST_GROUPS[0]['components'])
            host_groups[1]['components'].extend(DEFAULT_EXTRA_HOST_GROUPS[1]['components'])

    if hdp_version_tuple <= (2, 0, 13, 0): # APP_TIMELINE_SERVER not applicable for this version
        host_groups[0]['components'] = list(filter(lambda x: x.get('name') != 'APP_TIMELINE_SERVER',
                                                   host_groups[0]['components']))

    repo_url_host = 'http://public-repo-1.hortonworks.com'
    ambari_repo_url = ('{}/ambari/centos{}/{}.x/updates/{}/'
                       'ambari.repo'.format(repo_url_host, os_major_version, args.ambari_version[0],
                                            args.ambari_version))
    hdp_repo_url = ('{}/HDP/centos{}/{}.x/updates/{}'.format(repo_url_host, os_major_version, args.hdp_version[0],
                                                             args.hdp_version))

    for node in cluster:
        node.execute('wget -nv {} -O /etc/yum.repos.d/ambari.repo'.format(ambari_repo_url),
                     quiet=quiet)

    logger.info('Installing Ambari server and agents ...')
    primary_node.execute('yum -y install ambari-server', quiet=quiet)
    primary_node.execute('ambari-server setup -v -s', quiet=quiet)
    primary_node.execute('ambari-server start', quiet=quiet)

    for node in cluster:
        node.execute('yum -y install ambari-agent', quiet=quiet)
        ambari_agent_config = node.get_file(AMBARI_AGENT_CONFIG_FILE_PATH)
        node.put_file(AMBARI_AGENT_CONFIG_FILE_PATH,
                      re.sub(r'(hostname)=.*', r'\1={}'.format(primary_node.fqdn),
                             ambari_agent_config))
        node.execute('ambari-agent start', quiet=quiet)

    mysql_config_commands = [
        ('wget -nv -O /tmp/mysql-connector-java.tar.gz '
         'https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.45.tar.gz'),
        'gzip -d /tmp/mysql-connector-java.tar.gz',
        'tar -xf /tmp/mysql-connector-java.tar -C /tmp',
        ('cp /tmp/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar '
         '/tmp/mysql-connector-java.jar'),
        'ambari-server setup --jdbc-db=mysql --jdbc-driver=/tmp/mysql-connector-java.jar',
        'rm -rf /tmp/mysql-connector-java*'
    ]
    primary_node.execute(' && '.join(mysql_config_commands), quiet=quiet)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    hostname = ('localhost' if models.client.info().get('Name') == 'moby'
                else socket.getaddrinfo(socket.gethostname(), 0, flags=socket.AI_CANONNAME)[0][3])
    port = primary_node.host_ports.get(AMBARI_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Ambari server is now reachable at %s', server_url)

    ambari = Ambari(server_url, username=DEFAULT_AMBARI_USERNAME, password=DEFAULT_AMBARI_PASSWORD)

    logger.info('Waiting for all hosts to be visible in Ambari ...')
    def condition(ambari, cluster):
        cluster_hosts = {node.fqdn for node in cluster}
        ambari_hosts = {host.host_name for host in ambari.hosts}
        logger.debug('Cluster hosts: %s; Ambari hosts: %s', cluster_hosts, ambari_hosts)
        return cluster_hosts == ambari_hosts
    wait_for_condition(condition=condition, condition_args=[ambari, cluster])

    logger.info('Updating install repo to use %s HDP version ...', args.hdp_version)
    # based off of release notes of https://bit.ly/2R06NKp
    if stack_version_tuple >= (2, 6):
        url = join_url_parts(hdp_repo_url, 'build.id')
        response = requests.get(url)
        response.raise_for_status()
        build_number = next((int(item.split(':')[1].strip()) for item in response.text.split('\n')
                             if 'BUILD_NUMBER' in item), None)
        if not build_number:
            raise Exception('Could not determine build number as required for repo setting. Build data found: ',
                            response.text)

        # version_definitions not yet supported by Ambari client library - a TODO
        hdp_repo_version = '{}-{}'.format(args.hdp_version, build_number)
        version_definition = {
            'VersionDefinition': {
                'version_url': '{}/HDP-{}.xml'.format(hdp_repo_url, hdp_repo_version)
            }
        }
        url = join_url_parts(server_url, 'api', 'v1', 'version_definitions')
        data = json.dumps(version_definition)
        response = requests.post(url, data=data, auth=(DEFAULT_AMBARI_USERNAME, DEFAULT_AMBARI_PASSWORD),
                                 headers={'X-Requested-By': 'topology_hdp build'})
        response.raise_for_status()
    else:
        hdp_os = ambari.stacks('HDP').versions(stack_version).operating_systems('redhat6')
        hdp_os.repositories('HDP-{}'.format(stack_version)).update(base_url=hdp_repo_url, verify_base_url=False)
        hdp_repo_version = None
        build_number = None

    logger.info('Creating `cluster` with pre-defined components ...')
    ambari.blueprints('cluster').create(blueprint_name='cluster', stack_version=stack_version,
                                        stack_name='HDP', host_groups=host_groups)

    logger.info('Installing cluster components ...')
    hdp_cluster = ambari.clusters('cluster')
    # INSTALL_ONLY option not applicable for <= 2.0.13.0 ver, it will be install and start services.
    if hdp_version_tuple <= (2, 0, 13, 0):
        hdp_cluster = hdp_cluster.create(blueprint='cluster', default_password='hadoop',
                                         host_groups=DEFAULT_CLUSTER_HOST_MAPPING)
    elif hdp_repo_version:
        hdp_cluster = hdp_cluster.create(blueprint='cluster', default_password='hadoop',
                                         repository_version=hdp_repo_version,
                                         host_groups=DEFAULT_CLUSTER_HOST_MAPPING,
                                         provision_action='INSTALL_ONLY')
    else:
        hdp_cluster = hdp_cluster.create(blueprint='cluster', default_password='hadoop',
                                         host_groups=DEFAULT_CLUSTER_HOST_MAPPING,
                                         provision_action='INSTALL_ONLY')

    time.sleep(30) # Some versions of Ambari provide wrong status on wait. Need to slug some time.
    hdp_cluster.wait(timeout=5400, interval=30)

    logger.info('Waiting for all hosts to reach healthy state ...')
    def condition(ambari):
        health_report = hdp_cluster.health_report
        logger.debug('Ambari cluster health report: %s ...', health_report)
        return health_report.get('Host/host_state/HEALTHY') == len(list(ambari.hosts))
    wait_for_condition(condition=condition, condition_args=[ambari])

    logger.info('Waiting for components to be verified ...')
    def condition(ambari):
        comps = hdp_cluster.cluster.host_components.refresh()
        for comp in comps:
            if comp.state.upper() == 'UNKNOWN':
                logger.debug('Not ready with component `%s` ...', comp.component_name)
                return False
        else:
            return True
    wait_for_condition(condition=condition, condition_args=[ambari])

    hdp_services_state = set(service['state'] for service in hdp_cluster.services.to_dict())
    if 'STARTED' in hdp_services_state or 'STARTING' in hdp_services_state:
        logger.info('Ambari task queued to stop services ...')
        hdp_cluster.cluster.services.stop().wait()

    logger.info('Stopping Ambari for saving to Docker image ...')
    for node in cluster:
        node.execute('ambari-agent stop', quiet=quiet)

    primary_node.execute('ambari-server stop', quiet=quiet)
    primary_node.execute('service postgresql stop', quiet=quiet)

    for node in cluster:
        node.execute('; '.join(['yum clean all',
                                'cat /dev/null > ~/.bash_history && history -c']), quiet=quiet)

    repository = '{}/topology_hdp'.format(args.repository or defaults['DEFAULT_REPOSITORY'])
    tag_prefix = 'hdp{}_ambari{}'.format(args.hdp_version, args.ambari_version)
    primary_node_tag = '{}_{}'.format(tag_prefix, 'primary-node')
    secondary_node_tag = '{}_{}'.format(tag_prefix, 'secondary-node')

    logger.info('Committing the primary node container as %s %s', primary_node_tag,
                ('and pushing its image to {} ...'.format(repository) if args.push else '...'))
    primary_node.commit(repository=repository, tag=primary_node_tag, push=args.push)
    logger.info('Committing the secondary node container as %s %s', secondary_node_tag,
                ('and pushing its image to {} ...'.format(repository) if args.push else '...'))
    secondary_node.commit(repository=repository, tag=secondary_node_tag, push=args.push)

    if args.retain:
        logger.info('Starting Ambari ...')
        primary_node.execute('service postgresql start', quiet=quiet)
        primary_node.execute('ambari-server start', quiet=quiet)
        for node in cluster:
            node.execute('ambari-agent start', quiet=quiet)
    else:
        logger.info('Removing the containers ...')
        primary_node.stop()
        secondary_node.stop()
