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

    $ clusterdock topology_hdp start -h
    usage: clusterdock start [--always-pull] [--namespace ns] [--network nw]
                             [-o sys] [-r url] [-h] [--hdp-version ver]
                             [--ambari-version ver] [--predictable]
                             [--dont-start-cluster]
                             [--primary-node node [node ...]]
                             [--secondary-nodes node [node ...]]
                             topology
    
    Start a HDP cluster
    
    positional arguments:
      topology              A clusterdock topology directory
    
    optional arguments:
      --always-pull         Pull latest images, even if they're available locally
                            (default: False)
      --namespace ns        Namespace to use when looking for images (default:
                            None)
      --network nw          Docker network to use (default: cluster)
      -o sys, --operating-system sys
                            Operating system to use for cluster nodes (default:
                            None)
      -r url, --registry url
                            Docker Registry from which to pull images (default:
                            docker.io)
      -h, --help            show this help message and exit
    
    HDP arguments:
      --hdp-version ver     HDP version to use (default: 2.6.2)
      --ambari-version ver  Ambari version to use (default: 2.5.2.0)
      --predictable         If specified, attempt to expose container ports to the
                            same port number on the host (default: False)
      --dont-start-cluster  Don't start clusters in Apache Ambari (default: False)
    
    Node groups:
      --primary-node node [node ...]
                            Nodes of the primary-node group (default: ['node-1'])
      --secondary-nodes node [node ...]
                            Nodes of the secondary-nodes group (default:
                            ['node-2'])
