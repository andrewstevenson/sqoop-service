#!/bin/bash



sql_ddl="CREATE DATABASE sqoop_metastore;GRANT CREATE, UPDATE, DELETE, SELECT, ALTER, INDEX on sqoop_metastore.* to 'sqoop'@'localhost' identified by 'sqoop'"
e
echo ${sql_ddl} | mysql -u$1 -p

