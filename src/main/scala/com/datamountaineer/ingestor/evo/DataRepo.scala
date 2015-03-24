package com.datamountaineer.ingestor.evo

import com.beust.jcommander.JCommander
import org.apache.avro.Schema
import org.apache.hadoop.conf.{Configurable, Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.record.Record
import org.kitesdk.cli.Command
import org.kitesdk.cli.commands.CSVImportCommand
import org.kitesdk.data._
import org.kitesdk.data.spi.{DatasetRepositories, DatasetRepository, SchemaUtil}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

class DataRepo(schema: Schema, path: String, database: String, name: String) extends Configured {

  val log : Logger = LoggerFactory.getLogger(this.getClass)
  val dataset_path = path + "/" + database + "/" + name
  val root_path = path
  val repo : DatasetRepository = DatasetRepositories.repositoryFor("repo:hive:" + root_path)

  /**
   * Create a hive dataset in the repo
   * 
   * @param database The database to create the dataset(table) in.
   * @param name The dataset/table name                
   * */
  def create(database: String, name: String) :  Dataset[Record]  = {
    if (!repo.exists(database, name)) {
      log.info("Creating dataset at %s with schema %s".format(path, schema.toString(true)))
      repo.create(database, name, new DatasetDescriptor.Builder().format(Formats.PARQUET).schema(schema).build)
      repo.load(database, name).asInstanceOf[Dataset[Record]]
    } else {
      log.info("Dataset at %s already exists.".format(dataset_path))
      repo.load(database, name).asInstanceOf[Dataset[Record]]
    }
  }

  /**
   * Update a dataset with a merged avro schema
   * 
   * @param source_schema The new inbound schema to update the dataset with
   * @param dataset The dataset to update                     
   * */
  def merge(source_schema: Schema, dataset : Dataset[Record]) = {
    val target_descriptor = dataset.getDescriptor
    val target_schema = target_descriptor.getSchema

    if (!repo.exists(database, name)) log.error("Dataset %s not found".format(dataset_path))
    else {
      if (source_schema == target_schema) {
        log.info("No change in schemas detected.")
      }
      else {
        log.info("Change in schemas detected. Schemas will be merged and dataset updated.")
        log.debug("Source schema: %s".format(source_schema.toString(true)))
        log.debug("Target dataset schema: %s".format(target_schema.toString(true)))
        val new_schema = SchemaUtil.merge(source_schema, target_schema)
        log.debug("New merged schema: %s".format(new_schema.toString(true)))
        val updated_descriptor: DatasetDescriptor = new DatasetDescriptor.Builder(target_descriptor)
          .schema(new_schema)
          .build()
        repo.update(database, name, updated_descriptor)
      }
    }
  }

  /**
   * Load a HDFS directory containing CSV text into a dataset
   * 
   * @param input_schema A avro schema describing the source. Used to construct CSV header
   * @param input_path The directory in HDFS containing the CSV's
   * @param dataset The dataset to load
   * @param conf Configuration               
   * */
  def load_csv(input_schema: Schema, input_path: String, dataset : Dataset[Record], conf: Configuration ) = {
    val fs = new Path(input_path).getFileSystem(conf)
    val hdfs_path: Path = fs.makeQualified(new Path("hdfs:" + input_path))

    if (fs.exists(hdfs_path)) {
      val fields = input_schema.getFields
      val field_names = for (field <- fields) yield field.name()
      val header = field_names.mkString(",")
      val jc = new JCommander()
      jc.addCommand("csv-import", new CSVImportCommand(log))
      jc.parse("csv-import", hdfs_path.toString, dataset.getName, "--namespace", dataset.getNamespace, "--header", header)
      val parsed: String = jc.getParsedCommand
      val command: Command = jc.getCommands.get(parsed).getObjects.get(0).asInstanceOf[Command]
      (command.asInstanceOf[Configurable]).setConf(conf)
      command.run()
    } else {
      log.warn("No input directory found: %s.".format(hdfs_path.toString))
    }  
  }
}