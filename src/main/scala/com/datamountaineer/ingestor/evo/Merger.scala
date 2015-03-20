package com.datamountaineer.ingestor.evo

import org.apache.avro.Schema
import org.apache.hadoop.record.Record
import org.kitesdk.data._
import org.kitesdk.data.spi.{DatasetRepositories, DatasetRepository}
import org.slf4j.{Logger, LoggerFactory}

class Merger(schema: Schema, path: String, database: String, name: String) {
  val log : Logger = LoggerFactory.getLogger(this.getClass)
  val dataset_path = path + "/" + database + "/" + name
  val root_path = path
  val repo : DatasetRepository = DatasetRepositories.repositoryFor("repo:hive:" + root_path)
  val dataset : Dataset[Record]  = {
    if (!repo.exists(database, name)) {
      log.info("Creating dataset at %s with schema %s".format(path, schema.toString(true)))
      repo.create(database, name, new DatasetDescriptor.Builder().format(Formats.PARQUET).schema(schema).build)
      repo.load(database, name).asInstanceOf[Dataset[Record]]
    } else {
      log.info("Dataset at %s already exists.".format(dataset_path))
      repo.load(database, name).asInstanceOf[Dataset[Record]]
    }
  }

  def merge(source_schema: Schema) = {
    val target_descriptor = dataset.getDescriptor
    val target_schema = target_descriptor.getSchema
    //val new_schema = SchemaUtil.merge(source_schema, target_schema)
    log.info(source_schema.toString(true))

    if (!repo.exists(database, name)) log.error("Dataset %s not found".format(dataset_path))
    else {
      if (source_schema == target_schema) {
        log.info("No change in schemas detected.")
      }
      else {
        val updated_descriptor: DatasetDescriptor = new DatasetDescriptor.Builder(target_descriptor)
          .schema(source_schema)
          .build()
        //Datasets.update(dataset.getUri, updated_descriptor)
        repo.update(database, name, updated_descriptor)
      }
    }
  }

  def move(source: String, database: String, name: String, path: String) = {
    //    val hdfs_path = "hdfs:%s".format(source)
    //    val target =  get_uri(path, database, name)
    //    val args : Array[String] = Array(hdfs_path, target)
    //    val command : CSVImportCommand = new CSVImportCommand(log)
    //    new JCommander(command, args.toString)
    //    println(command.getExamples.toString)
    //    command.run()
  }
}