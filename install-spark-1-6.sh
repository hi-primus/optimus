#!/usr/bin/env bash
set -e
SPARK_VERSION='1.6.3'
HADOOP_VERSION='2.6'

curl http://apache.mirror.gtcomm.net/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
cd /tmp && tar -xvzf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
