#!/usr/bin/env bash
set -e

SPARK_VERSION='2.4.1'
HADOOP_VERSION='2.7'

curl https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
cd /tmp && tar -xvzf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
