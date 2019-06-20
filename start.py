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

# Ambari logon credentials: admin / admin
# Atlas logon credentials: admin / hadoop (3.X versions) and admin / admin (2.X versions)
# Kerberos is setup to work for >= 3.X versions only

import json
import logging
import os
import re
import socket
import textwrap
import time

from ambariclient.client import Ambari
from ambariclient.models import Request

from clusterdock.models import Cluster, client, Node
from clusterdock.utils import join_url_parts, print_topology_meta, version_tuple, wait_for_condition

# Files placed in this directory on primary_node are available
# in clusterdock_config_directory after cluster is started.
# Also, this gets volume mounted to all secondary nodes and hence available there too.
CLUSTERDOCK_CLIENT_CONTAINER_DIR = '/etc/clusterdock/client'
DEFAULT_NAMESPACE = 'clusterdock'

AMBARI_AGENT_CONFIG_FILE_PATH = '/etc/ambari-agent/conf/ambari-agent.ini'
AMBARI_PORT = 8080
HBASE_THRIFT_SERVER_PORT = 9090
HBASE_THRIFT_SERVER_INFO_PORT = 9095

DEFAULT_ATLAS_REST_PORT = 21000
DEFAULT_CLUSTER_NAME = 'cluster'

KERBEROS_CONFIG_CONTAINER_DIR = '/etc/clusterdock/client/kerberos'
KERBEROS_USER_PRINCIPAL_PASSWORD = 'acladmin'
KERBEROS_USER_PRINCIPAL_PREFIX = 'admin/admin'
KDC_HOSTNAME = 'kdc'
KDC_IMAGE = 'clusterdock/topology_nodebase_kerberos:centos6.6'
KDC_ACL_FILENAME = '/var/kerberos/krb5kdc/kadm5.acl'
KDC_CONF_FILENAME = '/var/kerberos/krb5kdc/kdc.conf'
KDC_KEYTAB_FILENAME = '{}/clusterdock.keytab'.format(KERBEROS_CONFIG_CONTAINER_DIR)
KDC_KRB5_CONF_FILENAME = '/etc/krb5.conf'
LINUX_USER_ID_START = 1000

logger = logging.getLogger('clusterdock.{}'.format(__name__))


def main(args):
    quiet = not args.verbose
    print_topology_meta(args.topology)

    if args.include_services and args.exclude_services:
            raise ValueError('Cannot pass both --include-services and --exclude-services.')

    clusterdock_config_host_dir = os.path.realpath(os.path.expanduser(args.clusterdock_config_directory))
    hdp_version_tuple = version_tuple(args.hdp_version)
    image_prefix = '{}/{}/topology_hdp:hdp{}_ambari{}'.format(args.registry,
                                                              args.namespace or DEFAULT_NAMESPACE,
                                                              args.hdp_version,
                                                              args.ambari_version)
    primary_node_image = '{}_{}'.format(image_prefix, 'primary-node')
    secondary_node_image = '{}_{}'.format(image_prefix, 'secondary-node')
    ports = [{AMBARI_PORT: AMBARI_PORT} if args.predictable else AMBARI_PORT]
    volumes = [{clusterdock_config_host_dir: CLUSTERDOCK_CLIENT_CONTAINER_DIR}]

    primary_node = Node(hostname=args.primary_node[0], group='primary', volumes=volumes,
                        image=primary_node_image, ports=ports)
    secondary_nodes = [Node(hostname=hostname, group='secondary', volumes=volumes, image=secondary_node_image)
                       for hostname in args.secondary_nodes]
    nodes = [primary_node] + secondary_nodes

    if args.kerberos:
        kerberos_config_host_dir = '{}/kerberos'.format(clusterdock_config_host_dir)
        volumes = [{kerberos_config_host_dir: KERBEROS_CONFIG_CONTAINER_DIR}]
        for node in nodes:
            node.volumes.extend(volumes)

        kdc_node = Node(hostname=KDC_HOSTNAME, group='kdc', image=KDC_IMAGE, volumes=volumes)

    cluster = Cluster(*nodes + ([kdc_node] if args.kerberos else []))
    for node in cluster.nodes:
        node.volumes.append({'/sys/fs/cgroup': '/sys/fs/cgroup'})
        # do not use tempfile.mkdtemp, as systemd wont be able to bring services up when temp ends to be created in
        # /var/tmp/ directory
        node.volumes.append(['/run', '/run/lock'])

    cluster.primary_node = primary_node
    cluster.secondary_nodes = secondary_nodes
    cluster_nodes = [cluster.primary_node] + cluster.secondary_nodes
    cluster.start(args.network, pull_images=args.always_pull)

    if args.kerberos:
        cluster.kdc_node = kdc_node
        _configure_kdc(cluster, args.kerberos_principals, args.kerberos_ticket_lifetime, quiet=quiet)
        _install_kerberos_clients(nodes, quiet=quiet)
        if args.kerberos_principals:
            _create_kerberos_cluster_users(nodes, args.kerberos_principals, quiet=quiet)

    logger.debug('Starting PostgreSQL for Ambari server ...')
    # Need this as init system in Docker misreports on postgres start initially
    # Check https://github.com/docker-library/postgres/issues/146 for more
    def condition():
        primary_node.execute('service postgresql restart', quiet=quiet)
        if '1 row' in primary_node.execute('PGPASSWORD=bigdata psql ambari '
                                           '-U ambari -h localhost -c "select 1"',
                                           quiet=quiet).output:
            return True
    wait_for_condition(condition=condition, time_between_checks=2)

    def condition():
        if 'running' in primary_node.execute('service postgresql status', quiet=quiet).output:
            return True
    wait_for_condition(condition=condition)

    time.sleep(10) # If images are set to start Ambari server/agents - give some time to recover the right status
    _update_node_names(cluster, quiet=quiet)

    # The HDP topology uses two pre-built images ('primary' and 'secondary'). If a cluster
    # larger than 2 nodes is started, some modifications need to be done.
    if len(secondary_nodes) > 1:
        _remove_files(nodes=secondary_nodes[1:], files=['/hadoop/hdfs/data/current/*'], quiet=quiet)

    logger.info('Starting Ambari server ...')
    primary_node.execute('ambari-server start', quiet=quiet)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    hostname = ('localhost' if client.info().get('Name') == 'moby'
                else socket.getaddrinfo(socket.gethostname(), 0, flags=socket.AI_CANONNAME)[0][3])
    port = cluster.primary_node.host_ports.get(AMBARI_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Ambari server is now reachable at %s', server_url)

    logger.info('Starting Ambari agents ...')
    for node in cluster_nodes:
        logger.debug('Starting Ambari agent on %s ...', node.fqdn)
        node.execute('ambari-agent start', quiet=quiet)

    ambari = Ambari(server_url, username='admin', password='admin')

    def condition(ambari, nodes):
        cluster_hosts = {node.fqdn for node in nodes}
        ambari_hosts = {host.host_name for host in ambari.hosts}
        logger.debug('Cluster hosts: %s; Ambari hosts: %s', cluster_hosts, ambari_hosts)
        return cluster_hosts == ambari_hosts
    wait_for_condition(condition=condition, condition_args=[ambari, cluster_nodes])

    service_types_to_leave = (args.include_services.upper().split(',')
                              if args.include_services else [])
    service_types_to_remove = (args.exclude_services.upper().split(',')
                               if args.exclude_services else [])
    if service_types_to_leave or service_types_to_remove:
        for service in list(ambari.clusters(DEFAULT_CLUSTER_NAME).services):
            service_name = service.service_name.upper()
            if (service_name in service_types_to_remove or
                (service_types_to_leave and service_name not in service_types_to_leave)):
                logger.info('Removing cluster service (name = %s) ...', service_name)
                service.delete()

    for node in secondary_nodes[1:]:
        logger.info('Adding %s to cluster ...', node.fqdn)
        ambari.clusters(DEFAULT_CLUSTER_NAME).hosts.create(node.fqdn)
        secondary_node = ambari.clusters(DEFAULT_CLUSTER_NAME).hosts(secondary_nodes[0].fqdn)
        for component in secondary_node.components:
            logger.debug('Adding component (%s) to cluster on host (%s) ...',
                         component.component_name, node.fqdn)
            host_components = ambari.clusters(DEFAULT_CLUSTER_NAME).hosts(node.fqdn).components
            host_components.create(component.component_name).wait()

        logger.debug('Installing all registered components on host (%s) ...', node.fqdn)
        ambari.clusters(DEFAULT_CLUSTER_NAME).hosts(node.fqdn).components.install().wait()

    logger.info('Waiting for all hosts to reach healthy state ...')
    def condition(ambari):
        health_report = ambari.clusters(DEFAULT_CLUSTER_NAME).health_report
        logger.debug('Ambari cluster health report: %s ...', health_report)
        return health_report.get('Host/host_state/HEALTHY') == len(list(ambari.hosts))
    wait_for_condition(condition=condition, condition_args=[ambari])

    service_names = [service['service_name'] for service in
                     ambari.clusters(DEFAULT_CLUSTER_NAME).services.to_dict()]

    if 'ATLAS' in service_names:
        logger.info('Configuring Atlas required properties ...')
        _configure_atlas(ambari, args.hdp_version, atlas_server_host=cluster.primary_node.fqdn)

    if 'HIVE' in service_names:
        primary_node.execute('touch /etc/hive/sys.db.created', quiet=quiet)

    if args.kerberos:
        logger.info('Configure Ambari for Kerberos ...')
        _configure_ambari_for_kerberos(ambari, cluster, args.kerberos_ticket_lifetime)

    logger.info('Waiting for components to be ready ...')
    def condition(ambari):
        comps = ambari.clusters(DEFAULT_CLUSTER_NAME).cluster.host_components.refresh()
        for comp in comps:
            if comp.state.upper() == 'UNKNOWN':
                logger.debug('Not ready with component `%s` ...', comp.component_name)
                return False
        else:
            return True
    wait_for_condition(condition=condition, condition_args=[ambari])

    if not args.dont_start_cluster:
        logger.info('Starting cluster services ...')
        ambari.clusters(DEFAULT_CLUSTER_NAME).services.start().wait(timeout=3600)

        if 'HBASE' in service_names:
            logger.info('Starting Thrift server ...')
            if hdp_version_tuple <= (2, 0, 13, 0):
                hbase_daemon_path = '/usr/lib/hbase/bin/hbase-daemon.sh'
            else:
                hbase_daemon_path = '/usr/hdp/current/hbase-master/bin/hbase-daemon.sh'
            primary_node.execute('{} start thrift -p {} '
                                 '--infoport {}'.format(hbase_daemon_path, HBASE_THRIFT_SERVER_PORT,
                                                        HBASE_THRIFT_SERVER_INFO_PORT), quiet=quiet)


def _update_node_names(cluster, quiet):
    logger.info('Stopping Ambari agents ...')
    nodes = [cluster.primary_node] + cluster.secondary_nodes
    for node in nodes:
        command = node.execute('ambari-agent stop', quiet=quiet)
        if command.exit_code != 0:
            raise Exception('Ambari agent on node ({}) returned non-zero exit code ({}) while '
                            'stopping. Full output:'
                            '\n{}'.format(node, command.exit_code,
                                          textwrap.indent(command.output, prefix='    ')))

    logger.info('Stopping Ambari server ...')
    command = cluster.primary_node.execute('ambari-server stop', quiet=quiet)
    if command.exit_code != 0:
        raise Exception('Ambari server returned non-zero exit code ({}) while '
                        'stopping. Full output:'
                        '\n{}'.format(command.exit_code,
                                      textwrap.indent(command.output,
                                                      prefix='    ')))

    logger.debug('Creating host name changes JSON ...')
    host_name_changes = {DEFAULT_CLUSTER_NAME: {'node-1.cluster': cluster.primary_node.fqdn,
                                                'node-2.cluster': cluster.secondary_nodes[0].fqdn}}
    cluster.primary_node.put_file('/root/host_name_changes.json', json.dumps(host_name_changes))
    command = ('yes | ambari-server update-host-names /root/host_name_changes.json > '
               '/var/log/ambari-server/ambari-server.out 2>&1')
    cluster.primary_node.execute(command, quiet=quiet)

    for node in nodes:
        ambari_agent_config = node.get_file(AMBARI_AGENT_CONFIG_FILE_PATH)

        logger.debug('Changing server hostname to %s ...', cluster.primary_node.fqdn)
        # ConfigObj.write returns a list of strings.
        node.put_file(AMBARI_AGENT_CONFIG_FILE_PATH,
                      re.sub(r'(hostname)=.*',
                             r'\1={}'.format(cluster.primary_node.fqdn),
                             ambari_agent_config))


def _remove_files(nodes, files, quiet):
    command = 'sh -c "rm -rf {}"'.format(' '.join(files))
    logger.info('Removing files (%s) from nodes (%s) ...',
                ', '.join(files),
                ', '.join(node.fqdn for node in nodes))
    for node in nodes:
        node.execute(command=command, quiet=quiet)


def ambari_get_current_config(ambari, cluster_name, config_type):
    items = ambari.clusters(cluster_name).configurations(config_type).items
    current_tag = items.parent.cluster.desired_configs[items.parent.type]['tag']
    items.inflate()
    current_item = {}
    for model in items._models:
        if model.tag == current_tag:
            current_item = (model.items[0] if (model.items and len(model.items) > 0) else {})
            break
    return current_item


def _configure_kdc(cluster, kerberos_principals, kerberos_ticket_lifetime, quiet):
    kdc_node = cluster.kdc_node

    logger.info('Updating KDC configurations ...')
    realm = cluster.network.upper()

    logger.debug('Updating krb5.conf ...')
    krb5_conf = cluster.kdc_node.get_file(KDC_KRB5_CONF_FILENAME)
    # Here '\g<1>' represents group matched in regex which is the original default value of ticket_lifetime.
    ticket_lifetime_replacement = kerberos_ticket_lifetime if kerberos_ticket_lifetime else '\g<1>'
    krb5_conf_contents = re.sub(r'EXAMPLE.COM', realm,
                                re.sub(r'example.com', cluster.network,
                                       re.sub(r'ticket_lifetime = ((.)*)',
                                              r'ticket_lifetime = {}'.format(ticket_lifetime_replacement),
                                              re.sub(r'kerberos.example.com',
                                                     kdc_node.fqdn,
                                                     krb5_conf))))
    kdc_node.put_file(KDC_KRB5_CONF_FILENAME, krb5_conf_contents)

    logger.debug('Updating kdc.conf ...')
    kdc_conf = kdc_node.get_file(KDC_CONF_FILENAME)
    max_time_replacement = kerberos_ticket_lifetime if kerberos_ticket_lifetime else '1d'
    kdc_node.put_file(KDC_CONF_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm,
                             re.sub(r'\[kdcdefaults\]',
                                    r'[kdcdefaults]\n max_renewablelife = 7d\n max_life = {}'.format(max_time_replacement),
                                    kdc_conf)))

    logger.debug('Updating kadm5.acl ...')
    kadm5_acl = kdc_node.get_file(KDC_ACL_FILENAME)
    kdc_node.put_file(KDC_ACL_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm, kadm5_acl))

    logger.info('Starting KDC ...')

    kdc_commands = [
        'kdb5_util create -s -r {} -P kdcadmin'.format(realm),
        'kadmin.local -q "addprinc -pw {} {}@{}"'.format(KERBEROS_USER_PRINCIPAL_PASSWORD,
                                                         KERBEROS_USER_PRINCIPAL_PREFIX, realm)
    ]
    if kerberos_principals:
        principals = ['{}@{}'.format(primary, realm)
                      for primary in kerberos_principals.split(',')]
        if kerberos_ticket_lifetime:
            kdc_commands.extend([('kadmin.local -q "addprinc -maxlife {}sec '
                                  '-maxrenewlife 5day -randkey {}"'.format(kerberos_ticket_lifetime, principal))
                                 for principal in principals])
        else:
            kdc_commands.extend(['kadmin.local -q "addprinc -randkey {}"'.format(principal)
                                 for principal in principals])
        kdc_commands.append('kadmin.local -q '
                            '"xst -norandkey -k {} {}"'.format(KDC_KEYTAB_FILENAME,
                                                               ' '.join(principals)))
    kdc_commands.extend(['service krb5kdc start',
                         'service kadmin start',
                         'authconfig --enablekrb5 --update',
                         'cp -f {} {}'.format(KDC_KRB5_CONF_FILENAME,
                                              KERBEROS_CONFIG_CONTAINER_DIR)])
    if kerberos_principals:
        kdc_commands.append('chmod 644 {}'.format(KDC_KEYTAB_FILENAME))

    kdc_node.execute(' && '.join(kdc_commands),
                     quiet=quiet)

    logger.info('Validating health of Kerberos services ...')

    def condition(node, services, quiet):
        services_with_poor_health = [service
                                     for service in services
                                     if node.execute(command='service {} status'.format(service),
                                                     quiet=quiet).exit_code != 0]
        if services_with_poor_health:
            logger.debug('Services with poor health: %s',
                         ', '.join(services_with_poor_health))
        # Return True if the list of services with poor health is empty.
        return not bool(services_with_poor_health)
    wait_for_condition(condition=condition, condition_args=[kdc_node,
                                                            ['krb5kdc', 'kadmin'],
                                                            quiet])


def _install_kerberos_clients(nodes, quiet):
    logger.info('Installing Kerberos libraries on Cluster nodes ...')
    for node in nodes:
        if node.execute('yum list installed yum-plugin-ovl', quiet=quiet).exit_code != 0:
            logger.debug('Installing yum-plugin-ovl to workaround https://git.io/vFvPW ...')
            node.execute('yum -y -q install yum-plugin-ovl', quiet=quiet)

        command = ('yum -y -q install openldap-clients krb5-libs krb5-workstation'
                   if node.group == 'primary'
                   else 'yum -y -q install krb5-libs krb5-workstation')
        node.execute(command=command, quiet=quiet)


def _create_kerberos_cluster_users(nodes, kerberos_principals, quiet):
    commands = ['useradd -u {} -g hadoop {}'.format(uid, primary)
                for uid, primary in enumerate(kerberos_principals.split(','),
                                              start=LINUX_USER_ID_START)]
    for node in nodes:
        node.execute('; '.join(commands), quiet=quiet)


def _configure_ambari_for_kerberos(ambari, cluster, kerberos_ticket_lifetime):
    # based off of https://github.com/apache/ambari/blob/trunk/ambari-server/docs/security/kerberos/enabling_kerberos.md
    cluster_nodes = [cluster.primary_node] + cluster.secondary_nodes
    hdp_cluster = ambari.clusters(DEFAULT_CLUSTER_NAME)
    realm = cluster.network.upper()
    ticket_lifetime_replacement = kerberos_ticket_lifetime if kerberos_ticket_lifetime else '24h'

    hdp_cluster.services.create('KERBEROS')
    hdp_cluster.services('KERBEROS').components.create('KERBEROS_CLIENT')

    data = [{
        'Clusters': {
            'desired_config': {
                'type': 'krb5-conf',
                'tag': str(time.time()),
                'properties': {
                    'domains': '',
                    'manage_krb5_conf': 'true',
                    'conf_dir': '/etc',
                    'content': ("[libdefaults]\n  renew_lifetime = 7d\n  forwardable = true\n  default_realm = {{realm}}\n  " +
                                ("ticket_lifetime = {}\n  ".format(ticket_lifetime_replacement)) +
                                "dns_lookup_realm = false\n  dns_lookup_kdc = false\n  default_ccache_name = /tmp/krb5cc_%{uid}\n  #default_tgs_enctypes = {{encryption_types}}\n  #default_tkt_enctypes = {{encryption_types}}\n{% if domains %}\n[domain_realm]\n{%- for domain in domains.split(',') %}\n  {{domain|trim()}} = {{realm}}\n{%- endfor %}\n{% endif %}\n[logging]\n  default = FILE:/var/log/krb5kdc.log\n  admin_server = FILE:/var/log/kadmind.log\n  kdc = FILE:/var/log/krb5kdc.log\n\n[realms]\n  {{realm}} = {\n{%- if master_kdc %}\n    master_kdc = {{master_kdc|trim()}}\n{%- endif -%}\n{%- if kdc_hosts > 0 -%}\n{%- set kdc_host_list = kdc_hosts.split(',')  -%}\n{%- if kdc_host_list and kdc_host_list|length > 0 %}\n    admin_server = {{admin_server_host|default(kdc_host_list[0]|trim(), True)}}\n{%- if kdc_host_list -%}\n{%- if master_kdc and (master_kdc not in kdc_host_list) %}\n    kdc = {{master_kdc|trim()}}\n{%- endif -%}\n{% for kdc_host in kdc_host_list %}\n    kdc = {{kdc_host|trim()}}\n{%- endfor -%}\n{% endif %}\n{%- endif %}\n{%- endif %}\n  }\n\n{# Append additional realm declarations below #}")
                }
            }
        }
    }, {
        'Clusters': {
            'desired_config': {
                'type': 'kerberos-env',
                'tag': str(time.time()),
                'properties': {
                    'kdc_type': 'mit-kdc',
                    'manage_identities': 'true',
                    'create_ambari_principal': 'true',
                    'manage_auth_to_local': 'true',
                    'install_packages': 'false',
                    'encryption_types': 'aes des3-cbc-sha1 rc4 des-cbc-md5',
                    'realm' : realm,
                    'kdc_hosts' : cluster.kdc_node.fqdn,
                    'master_kdc' : '',
                    'admin_server_host' : cluster.kdc_node.fqdn,
                    'executable_search_paths' : '/usr/bin, /usr/kerberos/bin, /usr/sbin, /usr/lib/mit/bin, /usr/lib/mit/sbin',
                    'service_check_principal_name' : '${cluster}-${short_date}',
                    'case_insensitive_username_rules' : 'false'
                }
            }
        }
    }]
    ambari.client.put(hdp_cluster.url, data=json.dumps(data))

    for node in cluster_nodes:
        host_components = ambari.clusters(DEFAULT_CLUSTER_NAME).hosts(node.fqdn).components
        host_components.create('KERBEROS_CLIENT').wait()

    kerberos_service = ambari.clusters(DEFAULT_CLUSTER_NAME).services('KERBEROS')
    data = {
        'RequestInfo': {
            'context': 'Install Kerberos Service',
            'operation_level': {
                'level': 'CLUSTER',
                'cluster_name': DEFAULT_CLUSTER_NAME
            }
        },
        'Body': {
            'ServiceInfo': {
                'state':'INSTALLED'
            }
        }
    }
    response = ambari.client.put(kerberos_service.url, data=json.dumps(data))
    request = Request(hdp_cluster.requests, href=response.get('href'), data=response.get('Requests'))
    request.wait()

    data = {
        'Credential': {
            'principal': '{}@{}'.format(KERBEROS_USER_PRINCIPAL_PREFIX, realm),
            'key': KERBEROS_USER_PRINCIPAL_PASSWORD,
            'type': 'temporary'
        }
    }
    # gets to: http://node-1.cluster:8080/api/v1/clusters/cluster/credentials/kdc.admin.credential
    url = join_url_parts(hdp_cluster.url, 'credentials', 'kdc.admin.credential')
    ambari.client.post(url, data=json.dumps(data))

    data = {
        'Clusters': {
            'security_type': 'KERBEROS'
        }
    }
    response = ambari.client.put(hdp_cluster.url, data=json.dumps(data))
    request = Request(hdp_cluster.requests, href=response.get('href'), data=response.get('Requests'))
    request.wait()


def _configure_atlas(ambari, hdp_version, atlas_server_host):
    hdp_version_tuple = version_tuple(hdp_version)
    stack_version = '{}.{}'.format(hdp_version_tuple[0], hdp_version_tuple[1])
    stack_version_tuple = (hdp_version_tuple[0], hdp_version_tuple[1])

    core_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('core-site').items
    core_site_items.create(properties_to_update={
        'hadoop.proxyuser.hdfs.groups': '*',
        'hadoop.proxyuser.hdfs.hosts': '*',
        'hadoop.proxyuser.hive.hosts': atlas_server_host,
        'hadoop.proxyuser.livy.groups': '*',
        'hadoop.proxyuser.livy.hosts': '*',
        'hadoop.proxyuser.root.groups': '*',
        'hadoop.proxyuser.root.hosts': atlas_server_host})
    # Doesnt work - delete not available in ambari client - can ignore for now, TODO
    # core_site_items.delete(properties=['hadoop.security.key.provider.path'])

    if stack_version_tuple < (3, 0):
        hdfs_data_dir_items = '/hadoop/hdfs/data,/run/hadoop/hdfs/data,/run/lock/hadoop/hdfs/data'
        hdfs_name_dir_items = '/hadoop/hdfs/namenode,/run/hadoop/hdfs/namenode,/run/lock/hadoop/hdfs/namenode'
    else:
        current_hdfs_site_properties = ambari_get_current_config(ambari,
                                                                 DEFAULT_CLUSTER_NAME, 'hdfs-site')['properties']
        hdfs_data_dir_items = current_hdfs_site_properties.get('dfs.datanode.data.dir', '')
        if '/run/hadoop/hdfs/data' not in hdfs_data_dir_items.split(','):
            hdfs_data_dir_items = ','.join(hdfs_data_dir_items.split(',') + ['/run/hadoop/hdfs/data'])
        hdfs_name_dir_items = current_hdfs_site_properties.get('dfs.namenode.name.dir', '')
        if '/run/hadoop/hdfs/namenode' not in hdfs_name_dir_items.split(','):
            hdfs_name_dir_items = ','.join(hdfs_name_dir_items.split(',') + ['/run/hadoop/hdfs/namenode'])
    hdfs_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hdfs-site').items
    hdfs_site_items.create(properties_to_update={
        'dfs.datanode.data.dir': hdfs_data_dir_items,
        'dfs.datanode.max.transfer.threads': 16384,
        'dfs.namenode.handler.count': 200,
        'dfs.namenode.name.dir': hdfs_name_dir_items,
        'dfs.namenode.safemode.threshold-pct': 1})
    # Doesnt work - delete not available in ambari client - can ignore for now, TODO
    # hdfs_site_items.delete(properties=['dfs.encryption.key.provider.uri'])

    yarn_env_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('yarn-env').items
    if stack_version_tuple < (3, 0):
        yarn_env_items.create(properties_to_update={'min_user_id': 500})
    else:
        yarn_env_items.create(properties_to_update={'min_user_id': 1000})

    yarn_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('yarn-site').items
    if stack_version_tuple < (3, 0):
        yarn_site_items.create(properties_to_update={
            'hadoop.registry.rm.enabled': True,
            'yarn.nodemanager.local-dirs': '/hadoop/yarn/local,/run/hadoop/yarn/local,/run/lock/hadoop/yarn/local',
            'yarn.nodemanager.log-dirs': '/hadoop/yarn/log,/run/hadoop/yarn/log,/run/lock/hadoop/yarn/log',
            'yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round': 0.5,
            'yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes': (
                'org.apache.tez.dag.history.logging.ats.TimelineCachePluginImpl,'
                'org.apache.spark.deploy.history.yarn.plugin.SparkATSPlugin'),
            'yarn.timeline-service.entity-group-fs-store.group-id-plugin-classpath': (
                '/usr/hdp/{{spark_version}}/spark/hdpLib/*'),
            'yarn.timeline-service.http-authentication.proxyuser.root.groups': '*',
            'yarn.timeline-service.http-authentication.proxyuser.root.hosts': atlas_server_host})
    else:
        current_yarn_site_properties = ambari_get_current_config(ambari,
                                                                 DEFAULT_CLUSTER_NAME, 'yarn-site')['properties']
        yarn_local_dir_items = current_yarn_site_properties.get('yarn.nodemanager.local-dirs', '')
        if '/run/hadoop/yarn/local' not in yarn_local_dir_items.split(','):
            yarn_local_dir_items = ','.join(yarn_local_dir_items.split(',') + ['/run/hadoop/yarn/local'])
        yarn_log_dir_items = current_yarn_site_properties.get('yarn.nodemanager.local-dirs', '')
        if '/run/hadoop/yarn/log' not in yarn_log_dir_items.split(','):
            yarn_log_dir_items = ','.join(yarn_log_dir_items.split(',') + ['/run/hadoop/yarn/log'])
        yarn_site_items.create(properties_to_update={
            'yarn.nodemanager.local-dirs': yarn_local_dir_items,
            'yarn.nodemanager.log-dirs': yarn_log_dir_items,
            'yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round': 0.5,
            'yarn.timeline-service.http-authentication.proxyuser.root.groups': '*',
            'yarn.timeline-service.http-authentication.proxyuser.root.hosts': atlas_server_host})

    tez_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('tez-site').items
    tez_site_items.create(properties_to_update={
        'tez.am.java.opts': '-server -Djava.net.preferIPv4Stack=true',
        'tez.history.logging.proto-base-dir': '/warehouse/tablespace/external/hive/sys.db',
        'tez.history.logging.service.class': 'org.apache.tez.dag.history.logging.proto.ProtoHistoryLoggingService',
        'tez.queue.name': 'default',
        'tez.session.am.dag.submit.timeout.secs': 600})

    hive_env_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hive-env').items
    hive_env_items.create(properties_to_update={'hive.atlas.hook': True})

    hive_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hive-site').items
    if stack_version_tuple < (3, 0):
        hive_site_items.create(properties_to_update={
            'hive.exec.post.hooks': ('org.apache.hadoop.hive.ql.hooks.ATSHook,'
                                     'org.apache.atlas.hive.hook.HiveHook'),
            'atlas.rest.address': 'http://{}:{}'.format(atlas_server_host, DEFAULT_ATLAS_REST_PORT)})
    else:
        hive_site_items.create(properties_to_update={
            'hive.compactor.worker.threads': 1,
            'hive.exec.post.hooks': ('org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook,'
                                     'org.apache.atlas.hive.hook.HiveHook'),
            'atlas.rest.address': 'http://{}:{}'.format(atlas_server_host, DEFAULT_ATLAS_REST_PORT)})

    hive_interactive_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hive-interactive-site').items
    hive_interactive_site_props = ambari_get_current_config(ambari, DEFAULT_CLUSTER_NAME, 'hive-site')['properties']
    hive_interactive_site_items.create(properties_to_update={
        'hive.metastore.uris': hive_interactive_site_props['hive.metastore.uris']})

    if stack_version_tuple >= (3, 0):
        hbase_env_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hbase-env').items
        hbase_env_items.create(properties_to_update={'hbase.atlas.hook': True})
        # Doesnt work - delete not available in ambari client - can ignore for now, TODO
        # hbase_env_items.delete(properties=['hbase_max_direct_memory_size'])

        hbase_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('hbase-site').items
        hbase_site_items.create(properties_to_update={
            'hbase.coprocessor.master.classes': 'org.apache.atlas.hbase.hook.HBaseAtlasCoprocessor',
            'hbase.regionserver.handler.count': 70,
            'hbase.regionserver.thread.compaction.small': 3,
            'phoenix.rpc.index.handler.count': 20})
        # Doesnt work - delete not available in ambari client - can ignore for now, TODO
        # hbase_site_items.delete(properties=['hbase.bucketcache.ioengine', 'hbase.bucketcache.percentage.in.combinedcache', 'hbase.bucketcache.size'])

    kafka_broker_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('kafka-broker').items
    kafka_broker_items.create(properties_to_update={
        'log.dirs': '/kafka-logs /kafka-logs,/run/kafka-logs',
        'offsets.topic.replication.factor': 1})

    current_mapred_site_properties = ambari_get_current_config(ambari, DEFAULT_CLUSTER_NAME, 'mapred-site')['properties']
    mapred_local_dir_items = current_mapred_site_properties.get('mapred.local.dir', '')
    if '/hadoop/mapred' not in mapred_local_dir_items.split(','):
        mapred_local_dir_items = ','.join(mapred_local_dir_items.split(',') + ['/hadoop/mapred'])
    if '/run/hadoop/mapred' not in mapred_local_dir_items.split(','):
        mapred_local_dir_items = ','.join(mapred_local_dir_items.split(',') + ['/run/hadoop/mapred'])
    mapred_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('mapred-site').items
    mapred_site_items.create(properties_to_update={'mapred.local.dir': mapred_local_dir_items})

    if stack_version_tuple < (3, 0):
        webhcat_site_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('webhcat-site').items
        webhcat_site_items.create(properties_to_update={
            'webhcat.proxyuser.root.groups': '*',
            'webhcat.proxyuser.root.hosts': atlas_server_host})

    zookeeper_connect_port = int(ambari_get_current_config(ambari, DEFAULT_CLUSTER_NAME, 'zoo.cfg')
                                 ['properties']['clientPort'])
    kafka_broker_port = int(ambari_get_current_config(ambari, DEFAULT_CLUSTER_NAME, 'kafka-broker')
                            ['properties']['port'])
    application_properties_items = ambari.clusters(DEFAULT_CLUSTER_NAME).configurations('application-properties').items
    application_properties_items.create(properties_to_update={
        'atlas.audit.hbase.zookeeper.quorum': atlas_server_host,
        'atlas.authorizer.impl': 'simple',
        'atlas.graph.index.search.solr.zookeeper-url': '{}:{}/infra-solr'.format(atlas_server_host,
                                                                                 zookeeper_connect_port),
        'atlas.graph.storage.hostname': atlas_server_host,
        'atlas.kafka.bootstrap.servers': '{}:{}'.format(atlas_server_host, kafka_broker_port),
        'atlas.kafka.zookeeper.connect': '{}:{}'.format(atlas_server_host, zookeeper_connect_port),
        'atlas.rest.address': 'http://{}:{}'.format(atlas_server_host, DEFAULT_ATLAS_REST_PORT)})
