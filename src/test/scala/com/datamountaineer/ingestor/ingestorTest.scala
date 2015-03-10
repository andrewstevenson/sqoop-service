package com.datamountaineer.ingestor

import com.cloudera.sqoop.SqoopOptions
import com.datamountaineer.ingestor.sqoop.IngestSqoop
import com.datamountaineer.ingestor.utils.Constants

class IngestorTest extends IngestorTestTrait {
  val test_job_name = "mysql:localhost:testdb:testtable"
  val MY_SQL: String = "mysql"
  val LOCALHOST: String = "localhost"
  val TEST_DB: String = "test_db"
  val TEST_TABLE: String = "test_table"
  val SPLIT_BY_COL: String = "id"
  val NUM_MAPPERS: String = "4"
  val input = MY_SQL + Constants.SPILT_DELIMITER + LOCALHOST + Constants.SPILT_DELIMITER + TEST_DB +
    Constants.SPILT_DELIMITER + TEST_TABLE + Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER +
    NUM_MAPPERS + Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER + "0"
  val connection_string: String = "jdbc:" + MY_SQL + "://" + LOCALHOST + "/" + TEST_DB
  val target_dir: String = SqoopTargetDirPreFix + "/" + LOCALHOST + "/" + TEST_DB + "/" + TEST_TABLE +
    "/run_date=YYYYMMDD"
  var ingest : IngestSqoop = _
  var options: SqoopOptions = _
}
