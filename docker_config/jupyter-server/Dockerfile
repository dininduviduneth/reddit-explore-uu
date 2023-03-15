FROM ubuntu:22.04

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install openjdk-8-jre-headless scala wget screen ssh

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/local/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz

ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
ENV PATH="${PATH}:/usr/local/spark/bin"
ENV SPARK_HOME="/usr/local/spark"
ENV SPARK_NO_DAEMONIZE="true"

RUN apt -y install python3-pip
RUN pip install Jupyter