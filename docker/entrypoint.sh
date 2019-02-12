#!/bin/bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

function onshutdown {
    /opt/hadoop/sbin/hadoop-daemon.sh stop datanode
    /opt/hadoop/sbin/hadoop-daemon.sh stop namenode
}

trap onshutdown SIGTERM
trap onshutdown SIGINT

# Hadoop shell scripts assume USER is defined
export USER="${USER:-$(whoami)}"

# allow HDFS access from outside the container
sed -i s/localhost/${HOSTNAME}/ /opt/hadoop/etc/hadoop/core-site.xml

 /opt/hadoop/bin/hadoop namenode -format -force
/opt/hadoop/sbin/hadoop-daemon.sh start namenode
/opt/hadoop/sbin/hadoop-daemon.sh start datanode
timeout 10 bash -c -- '/opt/hadoop/bin/hdfs dfsadmin -safemode wait' || \
    /opt/hadoop/bin/hdfs dfsadmin -safemode leave

tail -f /dev/null

onshutdown
