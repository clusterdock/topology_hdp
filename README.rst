============================
HDP topology for clusterdock
============================

This repository houses the **HDP** topology for `clusterdock`_.

.. _clusterdock: https://github.com/clusterdock/clusterdock

Usage
=====

Assuming you've already installed **clusterdock** (if not, go `read the docs`_),
you use this topology by cloning it to a local folder and then running commands
with the ``clusterdock`` script:

.. _read the docs: http://clusterdock.readthedocs.io/en/latest/

.. code-block:: console

    $ git clone https://github.com/clusterdock/topology_hdp.git
    $ pip3 install -r topology_hdp/requirements.txt
    $ clusterdock start topology_hdp

To see full usage instructions for the ``start`` action, use ``-h``/``--help``:

.. code-block:: console

    $ clusterdock start topology_hdp -h
    usage: clusterdock start [--always-pull] [-c name] [--namespace ns] [-n nw]
                             [-o sys] [-r url] [-h] [--hdp-version ver]
                             [--include-services svc1,svc2,...]
                             [--ambari-version ver] [--predictable]
                             [--dont-start-cluster]
                             [--exclude-services svc1,svc2,...]
                             [--secondary-nodes node [node ...]]
                             [--primary-node node [node ...]]
                             topology

    Start a HDP cluster

    positional arguments:
      topology              A clusterdock topology directory

    optional arguments:
      --always-pull         Pull latest images, even if they're available locally
                            (default: False)
      -c name, --cluster-name name
                            Cluster name to use (default: None)
      --namespace ns        Namespace to use when looking for images (default:
                            None)
      -n nw, --network nw   Docker network to use (default: cluster)
      -o sys, --operating-system sys
                            Operating system to use for cluster nodes (default:
                            None)
      -r url, --registry url
                            Docker Registry from which to pull images (default:
                            docker.io)
      -h, --help            show this help message and exit

    HDP arguments:
      --hdp-version ver     HDP version to use (default: 2.6.4.0)
      --ambari-version ver  Ambari version to use (default: 2.6.1.0)
      --include-services svc1,svc2,..., -i svc1,svc2,...
                            If specified, a comma-separated list of service types
                            to include in the HDP cluster (default: None)
      --exclude-services svc1,svc2,..., -x svc1,svc2,...
                            If specified, a comma-separated list of service types
                            to exclude from the HDP cluster (default: None)
      --predictable         If specified, attempt to expose container ports to the
                            same port number on the host (default: False)
      --dont-start-cluster  Don't start clusters in Apache Ambari (default: False)

    Node groups:
      --primary-node node [node ...]
                            Nodes of the primary-node group (default: ['node-1'])
      --secondary-nodes node [node ...]
                            Nodes of the secondary-nodes group (default:
                            ['node-2'])

To see full usage instructions for the ``build`` action, use ``-h``/``--help``:

.. code-block:: console

    $ clusterdock build topology_hdp -h
    usage: clusterdock build [-n nw] [-o sys] [-r url] [-h] [--push]
                             [--ambari-version ver] [--retain] [--hdp-version ver]
                             [--bare]
                             topology

    Build images for the HDP topology

    positional arguments:
      topology              A clusterdock topology directory

    optional arguments:
      -n nw, --network nw   Docker network to use (default: cluster)
      -o sys, --operating-system sys
                            Operating system to use for cluster nodes (default:
                            None)
      -r url, --repository url
                            Docker repository to use for committing images
                            (default: docker.io/clusterdock)
      -h, --help            show this help message and exit

    HDP arguments:
      --hdp-version ver     HDP version to use (default: 2.6.4.0)
      --ambari-version ver  Ambari version to use (default: 2.6.1.0)
      --bare                If specified, will build a bare minimum cluster with
                            mandatory services (default: False)
      --push                If specified, will push the built Docker image to
                            Docker registry (default: False)
      --retain              If specified, will retain (not remove) the built
                            Docker containers (default: False)
