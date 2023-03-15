FROM ubuntu:22.04

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install openjdk-8-jre-headless scala wget screen ssh

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz && \
    tar -xvf hadoop-3.3.1.tar.gz && \
    mv hadoop-3.3.1 /usr/local/hadoop && \
    rm hadoop-3.3.1.tar.gz

# Set up Hadoop configuration files
ENV HADOOP_HOME=/usr/local/hadoop
RUN mkdir -p $HADOOP_HOME/hadoop_data/hdfs/namenode && \
    mkdir -p $HADOOP_HOME/hadoop_data/hdfs/datanode && \
    mkdir $HADOOP_HOME/hadoop_data/hdfs/tmp
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY start-hdfs.sh /start-hdfs.sh
COPY stop-hdfs.sh /stop-hdfs.sh
RUN chmod +x /start-hdfs.sh && \
    chmod +x /stop-hdfs.sh

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/local/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz

ENV PATH="${PATH}:/usr/local/spark/bin"
ENV SPARK_HOME="/usr/local/spark"
ENV SPARK_NO_DAEMONIZE="true"

CMD ["bash", "-c", "/start-hdfs.sh && /usr/local/spark/sbin/start-master.sh & /usr/local/spark/sbin/start-worker.sh spark://spark-master:7077"]

