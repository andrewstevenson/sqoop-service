package com.datamountaineer.ingestor.sqoop

//import com.datamountaineer.ingestor.IngestorTest.unitTest
//import com.datamountaineer.ingestor.utils.Constants
//import com.datamountaineer.ingestor.{IngestorTest, IngestorTestTrait}
//import org.scalatest.BeforeAndAfter
//import org.scalatest.mock.MockitoSugar

//class InitialiserTest extends IngestorTestTrait with BeforeAndAfter with MockitoSugar {
//  val initialiser = Initialiser
//
//  "Should return a netezza query" should {
//    assert(initialiser.netezza_query === initialiser.get_query("netezza"))
//  }
//
//  "Should return a mysql query" should {
//    assert(initialiser.mysql_query === initialiser.get_query("mysql"))
//  }
//
//  "Should return a db_details with " should {
//    val db = initialiser.get_db_conf(Constants.MYSQL, IngestorTest.LOCALHOST, IngestorTest.TEST_DB)
//    db.get shouldBe a[DbConfig]
//
//    "db_details should have username and password set to sqoop" taggedAs(unitTest) in {
//      val cred = db.get.get_credentials()
//      assert("sqoop" === cred._1)
//      assert("sqoop" === cred._2)
//    }
//  }
//
////  "Should return a JDBC connection" should {
////    val conn = initialiser.get_conn(Constants.MYSQL, IngestorTest.LOCALHOST, IngestorTest.TEST_DB)
////    conn.get shouldBe a[Connection]
////  }
//}
