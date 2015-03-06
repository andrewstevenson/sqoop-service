package com.datamountaineer.ingestor

import com.cloudera.sqoop.SqoopOptions
import com.datamountaineer.ingestor.models.JobMetaStorage
import com.datamountaineer.ingestor.sqoop.ingestSqoop
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.sqoop.tool.ImportTool
import org.slf4j.LoggerFactory

object ingestor extends Configured with Tool {
  val log = LoggerFactory.getLogger("ingestor")
  private val STORAGE_IMPLEMENTATION_KEY = "sqoop.job.storage.implementations"
  private val STORAGE_IMPLEMENTATION_CLASS = "com.datamountaineer.sqoop.models.JobMetaStorage"


  /**
   * Processes a job, either executes it or creates it. If creation and valid SqoopOptions in required.
   *
   * @param job_name Name of the job to create (db_type:server:database:table)
   * @param run_type Either exec or create
   * @param sqoop_options A SqoopOptions to store in the metastore. If run_type is create not used to can be null
   * @return None
   */
  def process_sqoop(job_name: String, run_type: String, sqoop_options: Option[SqoopOptions] = None) = {
    val storage = new JobMetaStorage
    run_type match {
      case "create" =>
        storage.create(sqoop_options.asInstanceOf[SqoopOptions])
      case "exec" =>
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
      case _ => log.error("Unsupported operation %s!".format(run_type))
    }
  }

  def main(args: Array[String]) {
    if (args == null || args.length < 2) {
      System.out.println( """
                            |Usage: <sqoop:exec <job_name>
                          """.stripMargin)
      System.exit(0)
    }

    val job_type: String = args(0).toString

    job_type match {
      case "sqoop:exec" =>
        process_sqoop(args(1).toString, "exec")
        log.info(System.getenv("JAVA_HOME"))

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
      case "sqoop:create" =>
        //build SqoopOptions
        val sqoop_options: SqoopOptions = new ingestSqoop(args(1).toString, true).build_sqoop_options()
        val job_name = args(1).split(":").take(4)mkString ":"
        log.info("Trying to create job called %s'.".format(job_name))
        process_sqoop(job_name, "create", Some(sqoop_options))
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