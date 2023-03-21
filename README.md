# reddit-explore-uu
In this project we have used comments from reddit to play around with multiple functionalities of Apache Spark, HDFS and Docker. The main objective of the project was to get our hands dirty with setting up containerized clusters of Apache Spark, HDFS using Docker functionalities.

The main phases of the project were as follows:

1. Setup a multi-container Spark-HDFS cluster using ```docker-compose```.
2. Run multiple analysis on the dataset provided - [Reddit Comments](https://files.pushshift.io/reddit/comments/ "Reddict comments").
3. Choose few analysis pipelines and run experiments to find out the performance with variable worker nodes.

## 1. Setting up the multi-container Spark-HDFS cluster

The base architecture we setup looks as follows:

base-cluster.png
