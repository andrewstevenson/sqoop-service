#!/bin/sh

type=$1
job=$2
db_type=$2
server=$3
database=$4

export INGESTOR_CLASS="com.datamountaineer.sqoop.sqoop.ingestor"
export INITIALISER_CLASS="com.datamountaineer.sqoop.sqoop.initialiser"
export SQOOP_SERVICE_JAR=sqoop-service-assembly-0.1.jar
export SQOOP_HOME=/usr/lib/sqoop
export HADOOP_CLASSPATH=conf/sqoop-site.xml
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export LIBJARS=${SQOOP_HOME}/*:${SQOOP_HOME}/lib/*

#put sqoop_service_jar first otherwise slick spits alot of debug crap out. Picks the log level up from zookeeper.
export JAR_CLASSPATH=${SQOOP_SERVICE_JAR}:${LIBJARS}:${HADOOP_HOME}/*:${HADOOP_HOME}/lib/*


if [[ "${type}" == "create" || "${type}" == "exec" ]]
then
    echo "Running sqoop:${type} ${job}"
    hadoop jar ${SQOOP_SERVICE_JAR} ${INGESTOR_CLASS} sqoop:${type} ${job} -libjars ${LIBJARS}
 elif [[ "${type}" == "initialise" ]]
 then
    echo "Initialising ${server}/${database}"
    java -cp ${JAR_CLASSPATH} ${INITIALISER_CLASS} ${db_type} ${server} ${database}
 fi