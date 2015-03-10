package com.datamountaineer.ingestor

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import com.cloudera.sqoop.SqoopOptions
import com.datamountaineer.ingestor.models.{SqoopJobDAO, SqoopJobSearchParameters, SqoopJob, JobMetaStorage}
import com.datamountaineer.ingestor.rest.Failure
import com.datamountaineer.ingestor.sqoop.IngestSqoop
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.sqoop.tool.ImportTool
import org.slf4j.{MDC, LoggerFactory}

object ingestor extends Configured with Tool {
  val log = LoggerFactory.getLogger("ingestor")
  private val STORAGE_IMPLEMENTATION_KEY = "sqoop.job.storage.implementations"
  private val STORAGE_IMPLEMENTATION_CLASS = "com.datamountaineer.sqoop.models.JobMetaStorage"
  val batch_size = 10
  val conn_jobs = new SqoopJobDAO()

  /**
   * Dynamically sets the logfile name
   *
   * @param job_name Set the log file name for the job*/
  def set_logger(job_name: String) = {
    val context: LoggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
    val configurator: JoranConfigurator = new JoranConfigurator();
    configurator.setContext(context)
    context.reset()
    MDC.put("loggerFileName", job_name)
  }

    /**
      * Execute a sqoop job stored in the metastore
      *
      * @param job_name The name of the job to run
      * @param storage The storage pointing to the metastore
      */
  def execute_job (job_name: String, storage: JobMetaStorage) = {
    set_logger(job_name)
    //read back our sqoop jobs to create a sqoop options
    val stored_job_options = storage.read(job_name).getSqoopOptions
    //clone and set as parent. Sqoop uses this to reconstruct the job after execution.
    val cloned_opts: SqoopOptions = stored_job_options.clone().asInstanceOf[SqoopOptions]
    stored_job_options.setParent(cloned_opts)
    //make sure we use our metastore
    stored_job_options.getConf.set(STORAGE_IMPLEMENTATION_KEY, STORAGE_IMPLEMENTATION_CLASS)
    val tool = new ImportTool()
    //run the sqoop!!
    tool.run(stored_job_options)
  }

  /**
   * Execute all jobs (enabled) for a given database.
   * Jobs are batched and run in parallel. If no batch size set the default of 10 is used.
   *
   * @param database The database to find jobs for
   * @param storage The storage pointing to the metastore
   * */
  def execute_database(database : String, storage: JobMetaStorage, batch_size : Int = batch_size) = {
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
          //batch the jobs
          //List(List(job1, job2), List(job3,job4),.....
          val batched = jobs.iterator.sliding(batch_size).toList
          batched.foreach( sub_batch => {
            //run each job in parallel
            sub_batch.par.foreach( job => {
              execute_job(job_name = job.job_name, storage = storage)
            })
          })
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
    val storage = new JobMetaStorage
    run_type match {
      case "create" =>
        storage.create(sqoop_options.asInstanceOf[SqoopOptions])
      case "exec" =>
        database match {
          case _ => execute_job(job_name.toString, storage)
          case Some(database) => execute_database(database.toString, storage)
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
      case "sqoop:exec;job" =>
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
      ToolRunner.run(conf, ingestor, strings)
    }
    res
  }
}