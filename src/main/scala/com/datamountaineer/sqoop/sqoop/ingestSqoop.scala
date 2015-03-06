package com.datamountaineer.sqoop.sqoop

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.{FileLayout, IncrementalMode}
import org.slf4j.LoggerFactory


class ingestSqoop( input: String, incr: Boolean) {
  val log = LoggerFactory.getLogger(classOf[ingestSqoop])
  val params : Map[String, String] = extract_sqoop_params(input, incr)

  private val db_type: String = params.get("db_type").get
  private val server: String = params.get("server").get
  private val database: String = params.get("database").get
  private val table_name: String = params.get("table").get
  private val split_by: String = params.get("split_by").get
  private val mappers: Integer = params.get("mappers").get.toInt

  /**
   * Even a input string, split by : to create a map with parameters to create a SqoopOptions
   * @param input Input string to parse
   * @param incr  Boolean flag to specify if it's an increment import
   * @return A map of parameters to pass to SqoopOptions
   * */
  def extract_sqoop_params(input: String, incr: Boolean = false): Map[String, String] = {
    val params = input.split(":")

    if (incr && params.length != 8) {
      log.error("Expected 8 parameters for incremental imports. " +
        "db_type:server:database:table:split_by:mappers:check_col:last_value", new ArrayIndexOutOfBoundsException)
      throw new ArrayIndexOutOfBoundsException
    }

    val check_col = if (incr) params(6) else ""
    val last_val = if (incr) params(7) else ""

    Map(
      "db_type" -> params(0),
      "server" -> params(1),
      "database" -> params(2),
      "table" -> params(3),
      "split_by" -> params(4),
      "mappers" -> params(5),
      "check_col" -> check_col,
      "last_value" -> last_val
    )
  }

  def build_sqoop_options() : SqoopOptions = {
    val sqoop_options: SqoopOptions = new SqoopOptions()
    sqoop_options.setJobName(db_type + ":" + server + ":" + database + ":" + table_name)
    sqoop_options.setConnectString("jdbc:" +
      this.db_type + "://" +
      this.server + "/" +
      this.database)
    sqoop_options.setTableName(this.table_name)
    sqoop_options.setIncrementalMode(IncrementalMode.AppendRows)
    sqoop_options.setIncrementalTestColumn(params.get("check_col").get)
    sqoop_options.setIncrementalLastValue(params.get("last_value").get)
    sqoop_options.setAppendMode(true)
    sqoop_options.setSplitByCol(this.split_by)
    sqoop_options.setNumMappers(this.mappers)
    sqoop_options.setTargetDir("/data/lz/" +
      this.server + "/" +
      this.database + "/" +
      sqoop_options.getTableName + "/run_date=" +
      new SimpleDateFormat("YYYYMMdd").format(Calendar.getInstance().getTime))
    sqoop_options.setEscapedBy('\\')

    //avro/parquet not supported for netzza and teradata
    if (this.db_type == "netezza" ||
        this.db_type == "oracle" ||
        this.db_type == "teradata" ||
        this.db_type == "sqlserver" ||
        this.db_type == "mysql") {
      sqoop_options.setDirectMode(true)
      sqoop_options.setFileLayout(FileLayout.TextFile)
      //sqoop_options.setCompressionCodec("com.hadoop.compression.lzo.LzopCodec")
    }
    else {
      sqoop_options.setHiveDropDelims(true)
      sqoop_options.setFileLayout(FileLayout.AvroDataFile)
      sqoop_options.setCompressionCodec("org.apache.hadoop.io.compress.SnappyCodec")
    }
    sqoop_options.setUsername("sqoop")
    sqoop_options.setPasswordFilePath("/secure/" + server + "/" + database + ".conf")
    sqoop_options
  }
}

//class ingestSqoopOozie(incremental: Boolean = false, params: Map[String, String])
//  extends ingestSqoop(incremental, params) {
//  private val USER_NAME_TL: ThreadLocal[String] = new ThreadLocal[String]
//  private val USER_NAME: String = "user.name"
//  private val NAME_NODE: String = "name_node"
//  private val JOB_TRACKER: String = "job_tracker"
//  private val LIBPATH: String = "oozie.use.system.libpath"
//  private val WF_RERUN: String = "oozie.wf.rerun.failnodes"
//
//  val conf : Properties = new Properties()
//  var user_name: String = USER_NAME_TL.get
//  if (user_name == null) {
//    user_name = System.getProperty("user.name")
//  }
//
//  conf.setProperty("root_dir", "/core/applications/oozie/wrappers/ingest/sqoop/")
//  conf.setProperty("options_file", "${root_dir}/sqoop_options")
//  conf.setProperty("target_dir", "/DATA/staging/" + this.server + "/" + this.database + "/" + this.table + "/"
//    + "/run_date=" + this.timestamp.toString)
//  conf.setProperty("password_file", "${root_dir}/sqoop_options/" + this.server + "/" + this.database +
//    "/secure/conn.secure")
//  conf.setProperty("connection", this.connection_string)
//  conf.setProperty("target_dir", this.target_dir)
//  conf.setProperty("split_by", this.split_by)
//  conf.setProperty("num_mappers", this.mappers)
//
//  if (this.incr_flag) {
//    conf.setProperty(OozieClient.APP_PATH, "${root_dir}/sqoop_import_incr_workflow.xml")
//  }
//  else {
//    conf.setProperty(OozieClient.APP_PATH, "${root_dir}/sqoop_import_workflow.xml")
//    val where: String = ""
//    conf.setProperty("query", "SELECT * FROM " + this.table + where)
//  }
//
//  conf.setProperty(USER_NAME, user_name)
//  conf.setProperty(NAME_NODE, "hdfs://" + params.get("hdfs_name_service").toString)
//  conf.setProperty(JOB_TRACKER, params.get("yarn_name_service").toString)
//  conf.setProperty(LIBPATH, "true")
//  conf.setProperty(WF_RERUN, "true")
//}