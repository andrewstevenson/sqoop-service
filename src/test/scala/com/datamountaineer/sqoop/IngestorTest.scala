package com.datamountaineer.sqoop

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.IncrementalMode
import com.datamountaineer.sqoop.conf.Configuration
import com.datamountaineer.sqoop.sqoop.{Constants, ingestSqoop}
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConversions._

abstract class IngestorTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with Configuration

class IngestSqoopTest extends IngestorTest with BeforeAndAfter with MockitoSugar {
  private val MY_SQL : String = "mysql"
  private val LOCALHOST : String = "localhost"
  private val TEST_DB : String = "test_db"
  private val TEST_TABLE : String = "test_table"
  private val SPLIT_BY_COL : String = "id"
  private val NUM_MAPPERS : String = "4"
  val input = MY_SQL + Constants.SPILT_DELIMITER + LOCALHOST + Constants.SPILT_DELIMITER + TEST_DB +
    Constants.SPILT_DELIMITER + TEST_TABLE + Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER +
    NUM_MAPPERS +Constants.SPILT_DELIMITER + SPLIT_BY_COL + Constants.SPILT_DELIMITER + "0"
  val connection_string : String = "jdbc:" + MY_SQL + "://" + LOCALHOST + "/" + TEST_DB
  val target_dir : String = SqoopTargetDirPreFix + "/" + LOCALHOST + "/" + TEST_DB + "/" + TEST_TABLE +
    "/run_date=YYYYMMDD"

  val ingest = new ingestSqoop(input, true)
  val options = ingest.build_sqoop_options()

  "A `:` delimited input " + input should "create a SqoopOptions" in {
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
  it should "have connection string set to " + connection_string in {
    assert(options.getConnectString === connection_string)
  }
  it should "have table name set to " + TEST_TABLE in {
    assert(options.getTableName === TEST_TABLE)
  }
  it should "have direct mode set" in {
    assert(options.isDirect, true)
  }
  it should "be incremental" in {
    assert(options.getIncrementalMode === IncrementalMode.AppendRows)
  }
  it should "have check col set to " + SPLIT_BY_COL in {
    assert(options.getIncrementalTestColumn === SPLIT_BY_COL)
  }
  it should "have Hive delimiters unset" in {
    assert(options.getHiveDelimsReplacement === null)
  }
  it should "have password file should be set" in {
    assert(options.getPasswordFilePath != null)
  }
  it should "have target_dir set to " + target_dir in {
    assert(options.getTargetDir === target_dir)
  }
}