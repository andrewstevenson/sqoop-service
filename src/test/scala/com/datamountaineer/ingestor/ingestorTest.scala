package com.datamountaineer.ingestor

import com.datamountaineer.ingestor.utils.Constants
import org.scalatest.Tag

object IngestorTest extends IngestorTestTrait {

  val TEST_JOB_NAME = "mysql:localhost:testdb:testtable"
  val MY_SQL: String = "mysql"
  val LOCALHOST: String = "localhost"
  val TEST_DB: String = "testdb"
  val TEST_TABLE: String = "testtable"
  val SPLIT_BY_COL: String = "id"
  val NUM_MAPPERS: String = "4"
  val INPUT = MY_SQL + Constants.SPILT_DELIMITER + LOCALHOST + Constants.SPILT_DELIMITER + TEST_DB +
    Constants.SPILT_DELIMITER + TEST_TABLE + Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER +
    NUM_MAPPERS + Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER + "0"
  val CONNECTION_STRING: String = "jdbc:" + MY_SQL + "://" + LOCALHOST + "/" + TEST_DB
  val TARGET_DIR: String = SqoopTargetDirPreFix + "/" + LOCALHOST + "/" + TEST_DB + "/" + TEST_TABLE +
    "/run_date=YYYYMMDD"

  object unitTest extends Tag("com.datamountaineer.ingestor.ut")

}
