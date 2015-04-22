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
    sqoop_options.setConnectString("jdbc:" + db_type + "://" + server + "/" +  database)
    sqoop_options.setTableName(table_name)
    //check for incremental, if set to nothing default to full import
    if (!params.get(Constants.CHECK_BY_KEY).get.equals("")) {
      sqoop_options.setIncrementalMode(IncrementalMode.AppendRows)
      sqoop_options.setIncrementalTestColumn(params.get(Constants.CHECK_BY_KEY).get)
      sqoop_options.setIncrementalLastValue(params.get(Constants.LAST_VAL_KEY).get)
    }
    sqoop_options.setSplitByCol(split_by)
    sqoop_options.setNumMappers(mappers)
    sqoop_options.setTargetDir(SqoopTargetDirPreFix + "/" + server + "/" + database + "/" + sqoop_options.getTableName)
    sqoop_options.setEscapedBy('\\')

    //avro/parquet not supported for netzza and teradata
//    if (this.db_type == Constants.NETEZZA ||
//      this.db_type == Constants.ORACLE ||
//      this.db_type == Constants.TERADATA ||
//      this.db_type == Constants.SQL_SERVER) {
//      sqoop_options.setDirectMode(true)
//      sqoop_options.setFileLayout(FileLayout.TextFile)
//      sqoop_options.setCompressionCodec("com.hadoop.compression.lzo.LzopCodec")
//    }
//    else {
//
//      //switch to parquet once https://issues.apache.org/jira/browse/SQOOP-2252 is fixed
//      sqoop_options.setFileLayout(FileLayout.ParquetFile)
//      //sqoop_options.setCompressionCodec(Constants.SNAPPY_CODEC)
//    }
    //ignore direct mode now till can sort out text to parquet via kite
    sqoop_options.setDirectMode(false)
    sqoop_options.setFileLayout(FileLayout.ParquetFile)
    sqoop_options.setCompressionCodec(Constants.SNAPPY_CODEC)
    sqoop_options.setHiveDropDelims(true)
    sqoop_options.setNullNonStringValue("")
    sqoop_options.setNullStringValue("")
    sqoop_options
  }
}