
#!/bin/bash

# Update system packages
apt-get update -y && apt-get upgrade -y

# Python & pip
apt-get install -y python3 python3-pip

# Java (for Spark)
apt-get install -y default-jdk

# Git
apt-get install -y git

# Spark
SPARK_VERSION="3.4.1"
HADOOP_VERSION="3"
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:/opt/spark/bin" >> ~/.bashrc
source ~/.bashrc

# dbt
pip3 install dbt-bigquery

# Airflow (with BigQuery + GCS support)
pip3 install apache-airflow[gcp]==2.8.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.8.txt"

# Completed
echo "Spark, dbt, and Airflow installation completed."
