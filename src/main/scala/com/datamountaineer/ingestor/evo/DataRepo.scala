package com.datamountaineer.ingestor.evo

import com.beust.jcommander.JCommander
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.crunch.PipelineResult
import org.apache.hadoop.conf.{Configurable, Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.kitesdk.cli.Command
import org.kitesdk.cli.commands.CSVImportCommand
import org.kitesdk.data._
import org.kitesdk.data.spi.{DatasetRepositories, DatasetRepository, SchemaUtil}

import org.kitesdk.tools.CopyTask
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object DataRepo extends Configured {
  val log : Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a hive dataset in the repo
   * 
   * @param repo_root The root location of the repo
   * @param database The database to create the dataset(table) in.
   * @param name The dataset/table name
   * @return a Dataset
   * */
  def create_hive_dataset(repo_root: String,
                          database: String,
                          name: String,
                          schema: Schema) : Dataset[GenericData.Record] = {
    val repo : DatasetRepository = DatasetRepositories.repositoryFor("repo:hive:" + repo_root)
    
    if (!repo.exists(database, name)) {
      log.info("Creating dataset at %s with schema %s"
        .format(repo_root + "/" + database + "/" + name, schema.toString(true)))
      repo.create(database, name, new DatasetDescriptor.Builder().format(Formats.PARQUET).schema(schema).build)
      repo.load(database, name).asInstanceOf[Dataset[GenericData.Record]]
    } else {
      log.info("Dataset at %s already exists.".format(repo_root + "/" + database + "/" + name))
      val dataset = repo.load(database, name).asInstanceOf[Dataset[GenericData.Record]]
      val updated = update_schema(schema, dataset)
      updated match {
        case None => dataset
        case _ => updated.get
      }
    }
  }

  /**
   * Create a dataset under the specified path
   *
   * @param path The path to the create the dataset in.
   * @param storage_type The storage type for the dataset.
   * @return a Dataset
   * */
  def create_dataset(path: String, schema: Schema, storage_type : Format) :  Dataset[GenericData.Record]  = {
    val source_dataset: String = "dataset:hdfs:" + path

    if (!Datasets.exists(source_dataset.toString)) {
      Datasets.create(source_dataset, new DatasetDescriptor.Builder().format(storage_type)
        .schema(schema)
        .build, classOf[GenericData.Record])
    } else {
      Datasets.load(source_dataset.toString, classOf[GenericData.Record])
    }
  }

  /**
   * Update a dataset with a merged avro schema
   * 
   * @param source_schema The new inbound schema to update the dataset with
   * @param dataset The dataset to update
   * @return The updated dataset
   * */
  def update_schema(source_schema: Schema,
                    dataset : Dataset[GenericData.Record]) : Option[Dataset[GenericData.Record]] = {
    val target_descriptor = dataset.getDescriptor
    val target_schema = target_descriptor.getSchema

    if (!Datasets.exists(dataset.getUri)) {
      log.error("Dataset %s not found".format(dataset.getName))
      sys.exit()
      None
    }
    else {
      if (source_schema == target_schema) {
        log.info("No change in schemas detected.")
        None
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
        dataset.getDescriptor.getSchema
        Some(Datasets.update(dataset.getUri, updated_descriptor))
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
  def load_csv(input_schema: Schema, input_path: String, dataset : Dataset[GenericData.Record], conf: Configuration ) = {
    val fs = new Path(input_path).getFileSystem(conf)
    val hdfs_path: Path = fs.makeQualified(new Path("hdfs:" + input_path))

    if (fs.exists(hdfs_path)) {
      val fields = input_schema.getFields
      val field_names = for (field <- fields) yield field.name()
      val header = field_names.mkString(",")
      val jc = new JCommander()
      jc.addCommand("csv-import", new CSVImportCommand(log))
      jc.parse("csv-import", hdfs_path.toString, dataset.getName, "--namespace", dataset.getNamespace,
        "--header", header)//, "--transform", "com.datamountaineer.ingestor.transformations.TransformNULL")
      val parsed: String = jc.getParsedCommand
      val command: Command = jc.getCommands.get(parsed).getObjects.get(0).asInstanceOf[Command]
      command.asInstanceOf[Configurable].setConf(conf)
      command.run()
    } else {
      log.warn("No input directory found: %s.".format(hdfs_path.toString))
    }
  }

  /**
   * Load a an existing dataset into a target dataset
   *
   * @param input_dataset The input dataset
   * @param target_dataset The dataset to load
   * @param conf Configuration
   * @return an Int return code
   * */
  def load_dataset(input_dataset: Dataset[GenericData.Record],
                   target_dataset : Dataset[GenericData.Record],
                   conf: Configuration) : Pair[Int, Long] = {
    val source: View[GenericData.Record] = input_dataset
    val dest: View[GenericData.Record] = target_dataset
    conf.set("crunch.log.job.progress", "true")
    conf.set("crunch.debug", "true")

    val task: CopyTask[_] = new CopyTask[GenericData.Record](source, dest)
    task.setConf(conf)
    task.noCompaction
    val result: PipelineResult = task.run

    if (result.succeeded) {
      log.info("Added {} records to \"{}\"", task.getCount, target_dataset.getUri)
      Pair(0, task.getCount)
    }
    else {
      Pair(1,0)
    }
  }

  /**
   * Returns the dataset located at the input path
   *
   * @param input_path The location on the dataset
   * @return The dataset
   * */
  def get_dataset(input_path: String) :  Option[Dataset[GenericData.Record]] = {
      if (Datasets.exists("dataset:hdfs:" + input_path)) {
        Some(Datasets.load("dataset:hdfs:" + input_path, classOf[GenericData.Record]).asInstanceOf[Dataset[GenericData.Record]])
      } else {
        None
      }
  }
}