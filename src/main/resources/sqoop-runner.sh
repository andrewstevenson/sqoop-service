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
export HIVE_HOME=/usr/lib/hive
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export HADOOP_CLASSPATH=/etc/hive/conf/:${base}/../*:${base}/conf/sqoop-site.xml:${base}/../conf:${SQOOP_HOME}/*:${SQOOP_HOME}/lib/*:${HIVE_HOME}/lib/*
export JAR_CLASSPATH=/${base}/../conf/:${SQOOP_SERVICE_JAR}:${SQOOP_HOME}/*:${HADOOP_HOME}/*:${HADOOP_HOME}/lib/*
export HADOOP_USER_CLASSPATH_FIRST=true

if [[ "${type}" == "create" || "${type}" == "exec:job"  || "${type}" == "exec:database" ]]
then
    echo "Running sqoop:${type} ${job}"
    hadoop jar ${SQOOP_SERVICE_JAR} ${INGESTOR_CLASS} sqoop:${type} ${job}
elif [[ "${type}" == "initialise" ]]
then
echo "Initialising ${server}/${database}"
    java -cp ${JAR_CLASSPATH} ${INITIALISER_CLASS} ${db_type} ${server} ${database}
elif [[ "${type}" == "list:database" ]]
then
    java -cp  ${JAR_CLASSPATH} ${INGESTOR_CLASS} sqoop:list:database $2
    elif [[ "${type}" == "list:job" ]]
then
    java -cp  ${JAR_CLASSPATH} ${INGESTOR_CLASS} sqoop:list:job $2
else
    echo "Unknown run type " ${type}
fi