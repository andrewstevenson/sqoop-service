#!/bin/sh

target_server=$1
version=$2
target_dir=$3
me=$(echo $(whoami) | sed 's/europe\\//g')
#me="root"
sqoop_home=/usr/lib/sqoop

rm -f sqoop-service*.tar.gz
rm -r -f sqoop-service-${version}
mkdir -p sqoop-service-${version}
mkdir -p sqoop-service-${version}/bin
mkdir -p sqoop-service-${version}/conf
mkdir -p sqoop-service-${version}/lib
cp src/main/resources/*.sh sqoop-service-${version}/bin
chmod +x sqoop-service-${version}/bin/*
cp lib/*.jar sqoop-service-${version}/lib
cp src/main/resources/*.conf sqoop-service-${version}/conf
cp src/main/resources/*.xml sqoop-service-${version}/conf
cp src/main/resources/*.properties sqoop-service-${version}/conf
cp target/scala-2.10/sqoop-service-assembly-${version}.jar sqoop-service-${version}
tar -zcvf sqoop-service-${version}.tar.gz sqoop-service-${version}
scp sqoop-service-${version}.tar.gz ${me}@${target_server}:/${target_dir}
ssh ${me}:${me}@${target_server} "rm -f -r ${target_dir}/sqoop-service-${version};
tar -zxvf ${target_dir}/sqoop-service-${version}.tar.gz;"
#sx su -; cp ${target_dir}/sqoop-service-${version}/sqoop-service-assembly-${version}.jar ${sqoop_home}/lib"

