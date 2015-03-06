#!/bin/sh

type=$1
job=$2
db_type=$2
server=$3
database=$4

base=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export INGESTOR_CLASS="com.datamountaineer.ingestor.ingestor"
export INITIALISER_CLASS="com.datamountaineer.ingestor.sqoop.initialiser"
export SQOOP_SERVICE_JAR=${base}/../sqoop-service-assembly-0.1.jar
export SQOOP_HOME=/usr/lib/sqoop
export HADOOP_CLASSPATH=${base}/conf/sqoop-site.xml
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export LIBJARS=${SQOOP_HOME}/*
export JAR_CLASSPATH=${base}/../conf/:${base}/../lib/*:${base}/../*:${LIBJARS}:${HADOOP_HOME}/*:${HADOOP_HOME}/lib/*

#JAVA_OPTS_CONF=-Dconfig.file=${base}/../conf/application.conf

if [[ "${type}" == "create" || "${type}" == "exec" ]]
then
    echo "Running sqoop:${type} ${job}"
    hadoop jar ${SQOOP_SERVICE_JAR} ${INGESTOR_CLASS} sqoop:${type} ${job} -libjars ${LIBJARS}
 elif [[ "${type}" == "initialise" ]]
 then
    echo "Initialising ${server}/${database}"
    #java ${JAVA_OPTS_CONF}
    java -cp ${JAR_CLASSPATH} ${INITIALISER_CLASS} ${db_type} ${server} ${database}
 fi