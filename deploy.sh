#!/bin/sh

target_server=$1
version=$2
target_dir=$3

sqoop_home=/usr/lib/sqoop

rm -f sqoop-service*.tar.gz
rm -r -f sqoop-service-${version}
mkdir -p sqoop-service-${version}
cp src/main/resources/* sqoop-service-${version}
mkdir -p sqoop-service-${version}/bin
mkdir -p sqoop-service-${version}/conf
mkdir -p sqoop-service-${version}/lib
mv sqoop-service-${version}/*.sh sqoop-service-${version}/bin
chmod +x sqoop-service-${version}/bin/*
mv sqoop-service-${version}/*.* sqoop-service-${version}/conf
cp lib/*.jar sqoop-service-${version}/lib
cp target/scala-2.10/sqoop-service-assembly-${version}.jar sqoop-service-${version}
tar -zcvf sqoop-service-${version}.tar.gz sqoop-service-${version}
scp sqoop-service-${version}.tar.gz cloudera@${target_server}:/${target_dir}
ssh cloudera@${target_server} "rm -f -r ${target_dir}/sqoop-service-${version};
tar -zxvf ${target_dir}/sqoop-service-${version}.tar.gz;
sudo cp ${target_dir}/sqoop-service-${version}/sqoop-service-assembly-${version}.jar ${sqoop_home}/lib"

