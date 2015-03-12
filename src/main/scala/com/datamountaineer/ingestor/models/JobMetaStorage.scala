package com.datamountaineer.ingestor.models

import java.io.IOException
import java.util
import java.util.Properties

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.metastore.{JobData, JobStorage}
import com.cloudera.sqoop.tool.SqoopTool
import com.datamountaineer.ingestor.rest.Failure
import com.datamountaineer.ingestor.utils.Constants
import org.slf4j.LoggerFactory

//import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._
import scala.util.{Left, Right}

class JobMetaStorage() extends JobStorage  {
 // val log = LoggerFactory.getLogger(classOf[JobMetaStorage])
  val log = LoggerFactory.getLogger("JobMetaStorage")

  //get Data access objects for sqoop_jobs and sqpop_job_props
  var conn_jobs : SqoopJobDAO = _
  var conn_props : SqoopJobPropsDAO = _

  //called by sqoop to check it can use this connector
  def canAccept(descriptor: util.Map[String, String]): Boolean = {
    true
  }

  def set_conn_jobs(conn: SqoopJobDAO) = {
    conn_jobs = conn
  }

  def set_conn_props(conn: SqoopJobPropsDAO) = {
    conn_props = conn
  }



  //Called from sqoop.
  @throws(classOf[IOException])
  def open(descriptor: util.Map[String, String]) = {
    set_conn_jobs(new SqoopJobDAO)
    set_conn_props(new SqoopJobPropsDAO)
  }

  def open() = {
    set_conn_jobs(new SqoopJobDAO)
    set_conn_props(new SqoopJobPropsDAO)
  }

  /**
   * Read back a SqoopOptions from the metastore
   *
   * @param job_name The name of the job to look up in sqoop_job_props
   * @return A JobData with the SqoopOptions from the metastore
   * */
  @throws(classOf[IOException])
  def read(job_name: String): JobData = {
    //check if job exists. returns Pair[job_name, boolean]
    val job = check_if_exists(job_name = job_name)

    //check on second attribute to see if job exists
    job._2 match {
      case false =>
        log.error("Job " + job_name + " does not exist!")
        null
      case true =>
        //create search parameters for DAO
        val props = conn_props.search(SqoopJobPropParameters(job_name = Some(job_name)))
        //match on the return. Can either be Failure or a List of SqoopJobProps
        props match {
          case Left(failure: Failure) => log.error("Failed to find properties for %s.".format(job_name), new IOException)
            null
          case Right(props: List[SqoopJobProp]) =>
            val tool_name = Some(props.filter(
              p =>  {
                p.prop_class == Constants.PROPERTY_CLASS_SCHEMA &&
                p.prop_name == Constants.SQOOP_TOOL_KEY
            }).head.prop_val)

            //Bail if no tool name. Means job wasn't persisted correctly
            tool_name.getOrElse(log.error("Couldn't find tool name!"))

            val sqoop_options = props.filter(p => {
              p.prop_class == Constants.PROPERTY_CLASS_SQOOP_OPTIONS
            })

            val conf_options = props.filter(p => {
              p.prop_class == Constants.PROPERTY_CLASS_CONFIG
            })

            val tool = SqoopTool.getTool(tool_name.get)

            //set conf
            val conf: Configuration = new Configuration
            conf_options.foreach(f => {
              conf.set(f.prop_name, f.prop_val)
            })

            val sqoop_props : Properties = new Properties()
            sqoop_options.foreach(f => {
              sqoop_props.setProperty(f.prop_name, f.prop_val)
            })

            //now rebuild the sqoop options
            val opts: SqoopOptions = new SqoopOptions
            opts.setConf(conf)
            opts.loadProperties(sqoop_props)
            opts.setJobName(job_name)

            /*this property isn't stored by sqoop in metastore. not implementated in sqoop_option.writeproperties so reset it
             as when sqoop write backs the results of store jobs it checks if the job name or descriptor is null and bails
             */
            opts.setStorageDescriptor(mapAsJavaMap(Map( Constants.META_CONNECT_KEY -> opts.getConnectString)))
            new JobData(opts,tool)
        }
    }
  }

  /**
   * delete a job
   * 
   * @param job_name The name of the job to delete
   * */
  @throws(classOf[IOException])
  def delete(job_name: String): Unit = {
    conn_props.delete(job_name)
    conn_jobs.delete(job_name)
  }

  
  /**
   * Write a sqoop option
   * 
   * @param job_name Job name to write the property for
   * @param prop A SqoopJobProp to write.                
   * */
  def write_job_property(job_name: String, prop: SqoopJobProp) = {
    val prop_name = prop.prop_name
    val props = conn_props.search(SqoopJobPropParameters(job_name = Some(job_name), prop_name = Some(prop_name)))

    props match {
      case Right(props: List[SqoopJobProp]) =>
        conn_props.delete(job_name = job_name, prop_name = prop_name)
      case _ => log.info("Adding property %s.".format(prop_name))
    }
    conn_props.create(prop)
  }
  
  /**
   * Checks if a given jobs exists.
   * 
   * @param job_name The job name to check for in sqoop_jobs
   * @return A pair consisting of the job_id and a bool to indicate if it exists                
   * */
  def check_if_exists(job_name: String) : Pair[Long, Boolean] = {
    //check if jobs exists
    val job = conn_jobs.get(job_name)
    job match {
      case Left(failure) =>
       // log.warn(failure.message)
        Pair(-1, false)
      case Right(j: SqoopJob) =>
        Pair(j.id.get ,true)
    }
  }

  /**
   * Write a job to sqoop_jobs
   * 
   * @param job_name The name of the job
   * @param job_type The type of job (import)
   * @param enabled Flag indicating is the job is enabled                
   * 
   * */
  def write_job(job_name: String, job_type: String, enabled : Boolean) = {
    val job = conn_jobs.create(SqoopJob(
                job_type = job_type,
                job_name = job_name,
                server = job_name.split(":")(1),
                database = job_name.split(":")(2),
                table_name = job_name.split(":")(3),
                enabled = enabled)
              )

    job match {
      case Left(failure: Failure) =>
        log.error("Failed to create job %s. %s".format(job_name, failure.message), new IOException())
        false
      case _ => true
    }
  }
  
  /**
   * Write the SqoopOption properties to sqoop_job_props
   * 
   * @param job_id The job_id of an existing job entry in sqoop_jobs
   * @param job_name The job name
   * @param tool_name The name of the sqoop tool (import)                
   * @param sqoop_options The SqoopOptions to persist.
   * */
  def write_props(job_id: Long, job_name: String, tool_name: String, sqoop_options: SqoopOptions) = {
    //save name of tool
    write_job_property(
      job_name=job_name,
      SqoopJobProp(job_name=job_name,
      job_id=job_id,
      prop_name=Constants.SQOOP_TOOL_KEY,
      prop_val=tool_name,
      prop_class=Constants.PROPERTY_CLASS_SCHEMA))

    // Save the property set id.
    write_job_property(
      job_name=job_name,
      SqoopJobProp(job_name=job_name,
      job_id=job_id,
      prop_name=Constants.PROPERTY_SET_KEY,
      prop_val=Constants.CUR_PROPERTY_SET_ID,
      prop_class=Constants.PROPERTY_CLASS_SCHEMA))

    // Save all properties of the SqoopOptions.
    for (entry <- sqoop_options.writeProperties.entrySet())
      write_job_property(
        job_name=job_name,
        SqoopJobProp(job_name=job_name,
        job_id=job_id,
        prop_class= Constants.PROPERTY_CLASS_SQOOP_OPTIONS,
        prop_name = entry.getKey.toString ,
        prop_val = entry.getValue.toString
      ))

    // And save all unique properties of the configuration.
    val saveConf: Configuration = sqoop_options.getConf
    saveConf.set(Constants.STORAGE_IMPLEMENTATION_KEY, Constants.STORAGE_IMPLEMENTATION_CLASS)
    val baseConf: Configuration = new Configuration

    for (entry <- saveConf) {
      val key: String = entry.getKey
      val rawVal: String = saveConf.getRaw(key)
      val baseVal: String = baseConf.getRaw(key)
      if (baseVal != null && (rawVal != baseVal)) {
        write_job_property(
          job_name=job_name,
          SqoopJobProp(job_name=job_name,
            job_id=job_id,
            prop_class= Constants.PROPERTY_CLASS_CONFIG,
            prop_name = key,
            prop_val = rawVal
          ))
        log.info("Saving %s => %s / %s".format(key, rawVal, baseVal))
      }
    }
  }


  /**
   * Returns the job name used to identify the job. Built from connection string
   * 
   * @param sqoop_options The SqoopOptions for the job.
   * */
  def get_job_name(sqoop_options: SqoopOptions) : String = {
    sqoop_options.getConnectString.replace("jdbc:", "").replace("://", ":").replace("/", ":") + ":" + sqoop_options.getTableName
  }

  /**
   * Store a sqoop job in the metastore. Called by Sqoop when updating the job!
   *
   * @param job_name The name of the job to create
   * @param data The JobData containing the SqoopOptions to persist to sqoop_job_props                
   * */
  
  @throws(classOf[IOException])
  def create(job_name: String,  data: JobData): Unit = {
    val sqoop_options = data.getSqoopOptions
    val tool_name = data.getSqoopTool.getToolName
    val job: Pair[Long, Boolean] = check_if_exists(job_name: String)
    val new_job_name = get_job_name(sqoop_options)

    //check  if jobs exists (second parameter of job).
    job._2 match {
      case false =>
        write_job(job_name = new_job_name, job_type = tool_name, enabled = true)
      case true => log.info("Found job %s. Updating metastore.".format(job_name))
    }

    //messy checking job again
    val new_job: Pair[Long, Boolean] = check_if_exists(new_job_name: String)
    val job_id = if (new_job._2) new_job._1 else -1
    write_props(job_id=job_id, job_name = new_job_name, tool_name = tool_name, sqoop_options = sqoop_options)
  }


  /**
   * Store a sqoop job in the metastore
   * 
   * @param sqoop_options The SqoopOptions to persist to sqoop_job_props
   * */
  @throws(classOf[IOException])
  //own implementation
  def create(sqoop_options: SqoopOptions) = {
    val new_job_name = get_job_name(sqoop_options)
    val job : Pair[Long, Boolean]= check_if_exists(job_name = new_job_name)
    //if we don't have split by column disable the job. Might need a rethink at some point but initialiser to be improved
    val enable : Boolean = if (sqoop_options.getSplitByCol.isEmpty) false else true

    job._2 match {
      //no job found
      case false =>
        write_job(job_name = new_job_name, job_type = "import", enabled = enable)
        val job = check_if_exists(new_job_name)
        //did we persists the job ok?
        job._2 match {
          case true => write_props(job_id=job._1, job_name = new_job_name, tool_name = "import", sqoop_options = sqoop_options)
          case false => log.error("Job %s does not exists!".format(new_job_name), new IOException)
        }
        log.info("Job %s created".format(new_job_name))
      case true =>
        log.info("Job %s already exists!".format(new_job_name))
    }
  }

  /**
   * Updates a given job
   * @param job_name The name of the job to update
   * @param data The JobData to update the job with
   * */
  @throws(classOf[IOException])
  def update(job_name: String, data: JobData): Unit = {
    val job =  check_if_exists(job_name: String)
    job._2 match {
      case false => log.error("Job %s does not exist!".format(job_name))
      case true =>
        create(job_name=job_name, data = data)
    }
  }

  /**
   * Returns a list of jobs store in the metastore
   * */
  @throws(classOf[IOException])
  def list() : util.List[String] = {
    val jobs = conn_jobs.search(new SqoopJobSearchParameters())
    jobs match {
      case Right(jobs: List[SqoopJob]) =>
        if (jobs.size == 0) {
          log.warn("No jobs found!")
        }
        jobs.map(j => j.job_name)
      case Left(failure: Failure) =>
        log.warn("Some when wrong! %s".format(failure.message))
        val empty = list()
        empty
    }
  }
}
