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
COPY /usr/local/hadoop/sbin/start-dfs.sh /start-dfs.sh
COPY /usr/local/hadoop/sbin/stop-dfs.sh /stop-dfs.sh
RUN chmod +x /start-dfs.sh && \
    chmod +x /stop-dfs.sh

CMD ["bash", "-c", "/start-dfs.sh"]