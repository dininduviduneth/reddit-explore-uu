#!/bin/bash

# Start the NameNode
echo "Starting HDFS NameNode..."
hdfs --daemon start namenode

# Start the DataNodes
echo "Starting HDFS DataNodes..."
hdfs --daemon start datanode

# Print the status of HDFS services
echo "HDFS services status:"
hdfs dfsadmin -report


