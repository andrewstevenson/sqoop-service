package com.datamountaineer.ingestor

import com.cloudera.sqoop.SqoopOptions
import com.datamountaineer.ingestor.evo.Merger
import com.datamountaineer.ingestor.models.{JobMetaStorage, SqoopJob, SqoopJobDAO, SqoopJobSearchParameters}
import com.datamountaineer.ingestor.rest.Failure
import com.datamountaineer.ingestor.sqoop.IngestSqoop
import com.datamountaineer.ingestor.utils.Constants
import org.apache.avro.Schema
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.sqoop.manager.{DirectMySQLManager, DirectNetezzaManager, MySQLManager, NetezzaManager}
import org.apache.sqoop.orm.AvroSchemaGenerator
import org.slf4j.{Logger, LoggerFactory, MDC}



object Ingestor extends Configured with Tool with com.datamountaineer.ingestor.conf.Configuration {
  val log : Logger = LoggerFactory.getLogger("Ingestor")
  val batch_size = 10
  val conn_jobs = new SqoopJobDAO()
  val storage = new JobMetaStorage
  storage.open()
    /**
      * Execute a sqoop job stored in the metastore
      *
      * @param job_name The name of the job to run
      */
  def execute_job (job_name: String) = {
    MDC.put("loggerFileName", job_name)
    val job_data = storage.read(job_name)
    val options =  job_data.getSqoopOptions
    //clone and set as parent. Sqoop uses this to reconstruct the job after execution.
    val cloned_opts: SqoopOptions = options.clone().asInstanceOf[SqoopOptions]
      options.setParent(cloned_opts)
    //make sure we use our metastore
    options.getConf.set(Constants.STORAGE_IMPLEMENTATION_KEY, Constants.STORAGE_IMPLEMENTATION_CLASS)
    merge(options)

    //val tool = new ImportTool()
    //run the sqoop!!
    //tool.run(options)
  }

  def get_avro_schema(db_type : String, options: SqoopOptions) : Schema = {
    val conn = db_type match {
      case "mysql" => {
        if (options.isDirect) new MySQLManager(options) else new DirectMySQLManager(options)
      }
      case "netezza" => {
        if (options.isDirect) new NetezzaManager(options) else new DirectNetezzaManager(options)
      }
    }
    val avro = new AvroSchemaGenerator(options, conn, options.getTableName)
    log.info("Connecting to %s to generate Avro schema for %s".format(options.getConnectString, options.getTableName))
    avro.generate()
  }

  def merge(options: SqoopOptions) = {
    val database = options.getJobName.split(":")(2)
    val db_type = options.getJobName.split(":")(0)
    val schema = get_avro_schema(db_type, options)
    val repo_root = ScrubbedRepoDir
    val merger = new Merger(schema = schema, path = repo_root, database= database, name = options.getTableName)
    merger.merge(schema)
  }

  def batch (jobs : List[SqoopJob]) : List[Seq[SqoopJob]] = {
    jobs.iterator.sliding(batch_size).toList
  }

  def execute_batch(batched :List[Seq[SqoopJob]]) = {
    batched.foreach( sub_batch => {
      //run each job in parallel
      sub_batch.par.foreach( job => {
        execute_job(job_name = job.job_name)
      })
    })
  }

  /**
   * Execute all jobs (enabled) for a given database.
   * Jobs are batched and run in parallel. If no batch size set the default of 10 is used.
   *
   * @param database The database to find jobs for
   * */
  def execute_database(database : String, batch_size : Int = batch_size) = {
    val search = new SqoopJobSearchParameters(job_name=None, server = None, database = Some(database), table = None, enabled = Some(true))
    val jobs =  conn_jobs.search(search)
    jobs match {
      case Left(failure: Failure) => {
        log.error(failure.message)
      }
      case Right(jobs : List[SqoopJob]) => {
        if (jobs.size == 0) {
          log.warn("No jobs found for database %s".format(database))
        } else {
          execute_batch(batch(jobs))
        }
      }
    }
  }

  /**
   * Processes a job, either executes it or creates it. If creation and valid SqoopOptions in required.
   *
   * @param job_name Name of the job to create (db_type:server:database:table)
   * @param run_type Either exec or create
   * @param sqoop_options A SqoopOptions to store in the metastore. If run_type is create not used to can be null
   * @return None
   */
  def process_sqoop(database: Option[String] = None, job_name: Option[String] = None, run_type: String, sqoop_options: Option[SqoopOptions] = None) = {
    run_type match {
      case "create" =>
        storage.create(sqoop_options.asInstanceOf[SqoopOptions])
      case "exec" =>
        database match {
          case None => execute_job(job_name.get)
          case Some(database) => execute_database(database)
        }
      case _ => log.error("Unsupported operation %s!".format(run_type))
    }
  }

  def main(args: Array[String]) {
    if (args == null || args.length < 2) {
      System.out.println( """
                            |Usage: <sqoop:exec:job <job_name>
                            |Usage: <sqoop:exec:database <database> <batch_size>
                            |Usage: <sqoop:create:job <job_name>
                          """.stripMargin)
      System.exit(0)
    }

    val job_type: String = args(0).toString

    job_type match {
      case "sqoop:exec:job" =>
        process_sqoop( job_name = Some(args(1).toString), run_type = "exec")

        //            } else {
        //              //log.info("Sqoop successful!")
        //              //1 . Call Kite SDK to extract the avro schema from the csv
        //              //2.  Open staging kitedataset, create if doesn't exist
        //              //3.  Get DataSetDescriptor and schema
        //              //4.  Merge schemas (avro tools?)
        //              //5.  Update the schema in staging
        //              //5.  Run a crunch job to move from sqoop target dir to staging. Would like to be able to swap this
        //              // out...standard clean up?
        //            }

      case "sqoop:exec:database" => {
        process_sqoop(database = Some(args(1).toString), run_type = "exec")
      }
      case "sqoop:create:job" =>
        //build SqoopOptions
        val sqoop_options: SqoopOptions = new IngestSqoop(args(1).toString, true).build_sqoop_options()
        val job_name = args(1).split(":").take(4)mkString ":"
        log.info("Trying to create job called %s'.".format(job_name))
        process_sqoop(job_name = Some(job_name), run_type =  "create", sqoop_options = Some(sqoop_options))
      //log.info("Sqoop successful!")
      //              //1 . Call Kite SDK to extract the avro schema from the csv
      //              //2.  Open staging kitedataset, create if doesn't exist
      //              //3.  Get DataSetDescriptor and schema
      //              //4.  Merge schemas (avro tools?)
      //              //5.  Update the schema in staging
      //              //5.  Run a crunch job to move from sqoop target dir to staging. Would like to be able to swap this
      //              // out...standard clean up?


      case _ => log.error("Bollocks!")
    }
  }

  override def run(strings: Array[String]): Int = {
    val conf = new Configuration()
    val res = {
      ToolRunner.run(conf, Ingestor, strings)
    }
    res
  }
}