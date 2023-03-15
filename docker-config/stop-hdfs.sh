#!/bin/bash

echo "Stopping HDFS..."

# Stop the datanodes
docker stop hdfs-dn-a hdfs-dn-b

# Stop the namenode
docker stop hdfs-nn

echo "HDFS stopped."

