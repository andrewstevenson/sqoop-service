# Sqoop-Service
Sqoop scala driver with mysql metastore, slick db and spray.

Sqoop is an great tool for importing and exporting data in and out of HDFS. Most sqoop jobs are via scripting which is OK for ad-hoc jobs but when you want to automate and put it in a continuous integration path sometimes you need a compiled language and the benefits this gives you.

I choose scala to wrap my own code round the Java Sqoop code base. I simply extend the JobStorage class and use slick db to interact with a MySql database to store the job that Sqoop would normally store in the embedded HSQLDb.

By using a shared metastore we can sort of have a Sqoop2 but by using Sqoop 1 we get following benefits:

1.  Sqoop 1 is battle tested, feature rich with an active community.
2.  We can share jobs
3.  We have a central point for organisations to get and overview of the sqoop import/exports. Compliance and legal like this.
4.  Each time a source database server or database is registered we can setup jobs and set smart defaults. For example choose the correct connectors if available, set compression, use avro or parquet rather than text.
5.  Stop none supported configurations such Avro with direct mode. So use text with Lzop compression instead.
6.  We can hook into Cloudera's Kite SDK to handle schema evolution.

This component can perform the following:

1.  Initialises sqoop jobs in a shared MySQL backed metastore for either target RDBMS of type MySQL or Netezza. (Others easily added)
    This can also be rerun on a daily basis to pick up new tables on the target database.
2.  Create individual sqoop jobs and store them in the shared metastore. Choosing the optimal options where appropriate. Currently for MySQL Text and no direct mode is used. MySQL dump has no option for handling nulls so if you use the direct mode and have nulls in your target database you'll get NULL in you files which causes type conversion errors in Kite. To compromise I'll use Avro (better than TEXT) but this can only happen when https://issues.apache.org/jira/browse/SQOOP-2252 is fixed. By me.
3.  Exec stored sqoop jobs, creating HIVE external tables backed by Kite SDK.
4.  Exec all jobs in batches for a database.

For both points 2 and 3 Kite SDK is used to create and manage the HIVE tables. After each Sqoop Kite SDK is used to detect changes in the schema between the final HIVE external table and the target table that Sqoop imported. If a changes is detected the dataset/HIVE table is updated with the merged schema and the CSVImportCommand is triggered to transform the Sqoop'd CSV text into Parquet stored in the dataset/HIVE table.

##JobMetaStorage

This component extends the org.apache.sqoop.metastore.JobStorage to allow creation and execution of jobs via the SqoopTool interface. It uses slickDb to handle the interface with MYSQL. MYSQL can be swapped out for another db.

Some organisations only support MySQL, Oracle or Sql Server. This often gives the additional benefit of support for these RDBMS and backups. Also there are more and more RDBMS backing Hadoop services such as Hive, Oozie, Sentry and Cloudera Management Services, so consolidating makes senses rather then adding additional embedded database.

Additionaly I want to expose viewing, creating an changing jobs via Spray and AngualarJS so I want to be in control of the metastore definitions, hence I extend JobStorage.

You can also interact via the Sqoop CLI. For this to work you need to set the `sqoop.job.storage.implementations` properties in the sqoop-site.xml and add it to the HADOOP_CLASSPATH.

```
export HADOOP_CLASSPATH=sqoop-site.xml
```

```xml
<configuration>
 <property>
          <name>sqoop.metastore.client.enable.autoconnect</name>
          <value>true</value>
          <description>If true, Sqoop will connect to a local metastore
              for job management when no other metastore arguments are
              provided.
          </description>
      </property>
      <property>
          <name>sqoop.metastore.client.autoconnect.url</name>
          <value>jdbc:mysql://localhost:3306/sqoop_metastore?createDatabaseIfNotExist=false</value>
      </property>
      <property>
          <name>sqoop.metastore.client.autoconnect.username</name>
          <value>sqoop</value>
      </property>
      <property>
          <name>sqoop.metastore.client.autoconnect.password</name>
          <value>sqoop</value>
      </property>
      <property>
          <name>sqoop.job.storage.implementations</name>
          <value>com.datamountaineer.sqoop.models.JobMetaStorage</value>
      </property>
  </configuration>
```
## Spray Rest API

Use Spray to expose the metastore.

##AngularJS

Use AngularJS as a frontend to visual jobs and allow of use interaction.

## JobRunner

Maybe Akka to run the jobs? Overkill maybe.

## Schema Evolution

Kite SDK is used to create datasets in HIVE as external tables. The dataset is created with PARQUET as the storage format. Sqoop's AvroSchemaGenerator is used to generate an Avro schema from the target tables. The dataset is initially created with this schema.

Currently Sqoop doesn't set a default value and also creates the fields as a union ["db_type", null]. I swap this and also add a default. I've created [JIRA](https://issues.apache.org/jira/browse/SQOOP-2252) with Sqoop.

For subsequent runs the Avro schema generated by Sqoop (plus modifications) is compared against the target dataset. If a change is detected the datasets schema and HIVE table is updated.

#Install

To build

```
sbt assembly
```

This will build a jar in target/scala-2.10. I've set dependencies to be provided to keep the size down. They should be on your hadoop cluster anyway.

To deploy to your sqoop gateway machines I wrote a quick deploy script. Once jenkins or teamcity has built the jar call this script to tar and push the assembly, configs and runner scripts the sqoop gateway machine.

```
./deploy.sh <server> <version> <app directory on target edge node>
```
E.g.

```
./deploy.sh prd-edge-01 0.1 /opt/datamountaineer/
```

#Usage

NOTE: The sqoop username is set to `sqoop`. I'll make this configurable later.

The current setup accepts 4 run types:

##initialise

 This run type initialises jobs for each table found in the target database. It attempts to select the best column to split on and defaults the import type to incremental based on the split. For MySQL an auto increment column is looked for. On Netezza the distribution key is checked. It's important to note this is a best guess. If a suitable column is not found the job is tagged a disabled.

 There's always room for improvement here, should probably also check primary keys and their types. It's on a best effort basis for now.

To run the initialiser to pre-populate the jobs run `sqoop-runner.sh run_type db_type server database`. For example against a mysql run

```
bin/sqoop-runner.sh initialise mysql mysql-server target_database
```

##create

 This run type creates a job. It expects a `:` separated list of parameters in the form db_type:server:database:tablename:split_by_col:num_mappers:check_col:last_val

 The resulting job name will be db_type:server:database:tablename.

 ```
 bin/sqoop-runner.sh create mysql:localhost:my_db:my_table:my_auto_incr_col:4:my_auto_incr_col:0
 ```

##exec:job

This run type executes the job given as a parameter.

```
bin/sqoop-runner.sh exec:job mysql:localhost:my_db:my_table
```

##exec:database

This run type executes all enabled jobs for a database in parallel in batches of 10.


```
bin/sqoop-runner.sh exec:database my_database
```

#TODO

1.  More unit tests and integration.
2.  Convert jdbc to slick for initialiser
3.  Switch execution to use akka. Create a pool based on batch size and push sqoops to it. Akka routing maybe. Currently the code batches the list of jobs into a List[List[job1, job2, ..., job10], List[job11, job12]]. For each list in the list I execute the job in parallel using scala parallel collections but I'm still blocked by a long job potentially in the first list.
4.  Add sql server/oracle and teradata support.
5.  Support multiple incremental check cols?
6.  Clean up logging - remove log4j from classpath
7.  Web front end.