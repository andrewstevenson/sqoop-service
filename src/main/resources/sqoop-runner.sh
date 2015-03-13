#!/bin/sh

type=$1
job=$2
db_type=$2
server=$3
database=$4

base=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export INGESTOR_CLASS="com.datamountaineer.ingestor.Ingestor"
export INITIALISER_CLASS="com.datamountaineer.ingestor.sqoop.Initialiser"
export SQOOP_SERVICE_JAR=/${base}/../sqoop-service-assembly-0.1.jar
export SQOOP_HOME=/usr/lib/sqoop
export HADOOP_CLASSPATH=${SQOOP_SERVICE_JAR}:${base}/../conf:${SQOOP_HOME}/*:${SQOOP_HOME}/lib/*:${base}/conf/sqoop-site.xml
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export JAR_CLASSPATH=/${base}/../conf/:${SQOOP_SERVICE_JAR}:${SQOOP_HOME}/*:${HADOOP_HOME}/*:${HADOOP_HOME}/lib/*

#${SQOOP_HOME}/lib/*
if [[ "${type}" == "create" || "${type}" == "exec:job" ]]
then
    echo "Running sqoop:${type}:job ${job}"
    hadoop jar ${SQOOP_SERVICE_JAR} ${INGESTOR_CLASS} sqoop:${type} ${job}
elif [[ "${type}" == "exec:database" ]]
then
    echo "Running sqoop:${type}:database ${job}"
    hadoop jar ${SQOOP_SERVICE_JAR} ${INGESTOR_CLASS} sqoop:${type} ${job}
elif [[ "${type}" == "initialise" ]]
then
echo "Initialising ${server}/${database}"
    java -cp ${JAR_CLASSPATH} ${INITIALISER_CLASS} ${db_type} ${server} ${database}
fi