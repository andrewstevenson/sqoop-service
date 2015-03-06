package com.datamountaineer.ingestor.sqoop

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.IncrementalMode
import com.datamountaineer.ingestor.conf.Configuration
import com.datamountaineer.ingestor.utils.Constants
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

abstract class IngestorTest extends FunSuite with Matchers with OptionValues with Inside with Inspectors with Configuration

class IngestSqoopTest extends IngestorTest with BeforeAndAfter with MockitoSugar  {
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
  var ingest : ingestSqoop = _
  var options: SqoopOptions = _

  before {
    ingest = new ingestSqoop(input, true)
    options = ingest.build_sqoop_options()
  }

  test("SqoopOptions from an input") {
    val map = mapAsJavaMap(ingest.params)
    map should have size 8
    map should contain(Entry(Constants.DB_TYPE_KEY, MY_SQL))
    map should contain(Entry(Constants.SERVER_KEY, LOCALHOST))
    map should contain(Entry(Constants.DATABASE_KEY, TEST_DB))
    map should contain(Entry(Constants.TABLE_KEY, TEST_TABLE))
    map should contain(Entry(Constants.SPLIT_BY_KEY, SPLIT_BY_COL))
    map should contain(Entry(Constants.MAPPERS_KEY, NUM_MAPPERS))
    map should contain(Entry(Constants.SPLIT_BY_KEY, SPLIT_BY_COL))
    map should contain(Entry(Constants.LAST_VAL_KEY, "0"))
    options shouldBe a [SqoopOptions]
  }
  test("Should have connection string set") {
    assert(options.getConnectString === connection_string)
  }
  test("Should have table name set") {
    assert(options.getTableName === TEST_TABLE)
  }
  test("Should have direct mode set") {
    assert(options.isDirect, true)
  }
  test("Should be incremental") {
    assert(options.getIncrementalMode === IncrementalMode.AppendRows)
  }
  test("Should have check col set") {
    assert(options.getIncrementalTestColumn === SPLIT_BY_COL)
  }
  test("Should have Hive delimiters unset") {
    assert(options.getHiveDelimsReplacement === null)
  }
  test("Should have password file should be set"){
    assert(options.getPasswordFilePath != null)
  }
  test("Should have target_dir set") {
    assert(options.getTargetDir === target_dir)
  }
}