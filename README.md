# Sqoop-Service
Sqoop scala driver with mysql metastore, slick db and spray.

Sqoop is an great tool for import and exporting data in and out of HDFS. Most sqoop jobs are via scripting which is OK for ad-hoc jobs but when you want to automate and put it in a continuous integration path sometimes you need a compiled language and the benefits this gives you.

I choose scala to wrap my own code round the Java Sqoop code base. I simply extend the JobStorage class and use slick db to interact with a MySql database to store the job that Sqoop would normally store in the embedded HSQLDb.

By using a shared metastore we can sort of have a Sqoop2 but by using Sqoop 1 we get following benefits:

1.  Sqoop 1 is battle tested, feature rich with an active community.
2.  We can share jobs
3.  We have a central point for organisations to get and overview of the sqoop import/exports. Compliance and legal like this.
4.  Each time a source database server or database is registered we can setup jobs and set smart defaults. For example choose the correct connectors if available, set compression, use avro or parquet rather than text.
5.  Stop none supported configurations such Avro with direct mode. So use text with Lzop compression instead.

##JobMetaStorage

This component extends the org.apache.sqoop.metastore.JobStorage to allow creation and execution of jobs via the SqoopTool interface. It uses slickDb to handle the interface with MYSQL. MYSQL can be swapped out for another db.

Using MySQL, Postgres or Oracle means in corporate world that back up and admin is taken care of.

Since I want to expose viewing, creating an changing jobs via a web gui via spray I want to be in control of the metastore definitions, hence I extend JobStorage.

You can also interact with via the Sqoop CLI. For this to work you need to set the `sqoop.job.storage.implementations` properties in the sqoop-site.xml and add it to the HADOOP_CLASSPATH.

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

Use Kite SDK to take text files, extract schema, merge schema with target dataset and write text to avro/parquet dataset.
