package com.datamountaineer.ingestor.sqoop

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.{FileLayout, IncrementalMode}
import com.datamountaineer.ingestor.conf.Configuration
import com.datamountaineer.ingestor.utils.Constants
import org.slf4j.{Logger, LoggerFactory}


//noinspection ScalaDeprecation
class IngestSqoop(input: String, incr: Boolean) extends Configuration {
  val log : Logger = LoggerFactory.getLogger(this.getClass)
  val params: Map[String, String] = extract_sqoop_params(input, incr)

  private val db_type: String = params.get(Constants.DB_TYPE_KEY).get
  private val server: String = params.get(Constants.SERVER_KEY).get
  private val database: String = params.get(Constants.DATABASE_KEY).get
  private val table_name: String = params.get(Constants.TABLE_KEY).get
  private val split_by: String = params.get(Constants.SPLIT_BY_KEY).get
  private val mappers: Integer = params.get(Constants.MAPPERS_KEY).get.toInt

  /**
   * Even a input string, split by : to create a map with parameters to create a SqoopOptions
   * @param input Input string to parse
   * @param incr  Boolean flag to specify if it's an increment import
   * @return A map of parameters to pass to SqoopOptions
   **/
  def extract_sqoop_params(input: String, incr: Boolean = false): Map[String, String] = {
    val params = input.split(Constants.SPILT_DELIMITER)

    if (incr && params.length != 8) {
      log.error("Expected 8 parameters for incremental imports. " +
        "db_type:server:database:table:split_by:mappers:check_col:last_value", new ArrayIndexOutOfBoundsException)
      throw new ArrayIndexOutOfBoundsException
    }

    val check_col = if (incr) params(6) else ""
    val last_val = if (incr) params(7) else ""

    Map(
      Constants.DB_TYPE_KEY -> params(0),
      Constants.SERVER_KEY -> params(1),
      Constants.DATABASE_KEY -> params(2),
      Constants.TABLE_KEY -> params(3),
      Constants.SPLIT_BY_KEY -> params(4),
      Constants.MAPPERS_KEY -> params(5),
      Constants.CHECK_BY_KEY -> check_col,
      Constants.LAST_VAL_KEY -> last_val
    )
  }

  /**
   * Return a SqoopOptions based on the input params
   *
   * @return A SqoopOptions
   * */
  def build_sqoop_options(): SqoopOptions = {
    val sqoop_options: SqoopOptions = new SqoopOptions()
    sqoop_options.setJobName(db_type + Constants.SPILT_DELIMITER + server + Constants.SPILT_DELIMITER + database +
      Constants.SPILT_DELIMITER + table_name)
    sqoop_options.setConnectString("jdbc:" +
      this.db_type + "://" +
      this.server + "/" +
      this.database)
    sqoop_options.setTableName(this.table_name)
    //check for incremental, if set to nothing default to full import
    if (!params.get(Constants.CHECK_BY_KEY).get.equals("")) {
      sqoop_options.setIncrementalMode(IncrementalMode.AppendRows)
      sqoop_options.setIncrementalTestColumn(params.get(Constants.CHECK_BY_KEY).get)
      sqoop_options.setIncrementalLastValue(params.get(Constants.LAST_VAL_KEY).get)
    }
    sqoop_options.setSplitByCol(this.split_by)
    sqoop_options.setNumMappers(this.mappers)
    sqoop_options.setTargetDir(SqoopTargetDirPreFix + "/" +
      this.server + "/" +
      this.database + "/" +
      sqoop_options.getTableName)
    sqoop_options.setEscapedBy('\\')

    //avro/parquet not supported for netzza and teradata
//    if (this.db_type == Constants.NETEZZA ||
//      this.db_type == Constants.ORACLE ||
//      this.db_type == Constants.TERADATA ||
//      this.db_type == Constants.SQL_SERVER) {
//      sqoop_options.setDirectMode(true)
//      sqoop_options.setFileLayout(FileLayout.TextFile)
//      //sqoop_options.setCompressionCodec("com.hadoop.compression.lzo.LzopCodec")
//    }
//    else {
//      sqoop_options.setHiveDropDelims(true)
//      //switch to avro once https://issues.apache.org/jira/browse/SQOOP-2252 is fixed
//      sqoop_options.setFileLayout(FileLayout.TextFile)
//      //sqoop_options.setCompressionCodec(Constants.SNAPPY_CODEC)
//    }
    //ignore direct mode now till can sort out text to parquet via kite
    sqoop_options.setDirectMode(false)
    sqoop_options.setFileLayout(FileLayout.ParquetFile)
    sqoop_options.setCompressionCodec(Constants.SNAPPY_CODEC)
    sqoop_options.setHiveDropDelims(true)
    sqoop_options.setUsername("sqoop")
    sqoop_options.setPasswordFilePath("/secure/" + server + "/" + database + ".conf")
    sqoop_options.setNullNonStringValue("")
    sqoop_options.setNullStringValue("")
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