package com.datamountaineer.ingestor.models


import com.cloudera.sqoop.metastore.JobData
import com.datamountaineer.ingestor.IngestorTest.unitTest
import com.datamountaineer.ingestor.rest.{FailureType, Failure}
import com.datamountaineer.ingestor.{IngestorTest, IngestorTestTrait}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

class JobMetaStorageTest extends IngestorTestTrait with BeforeAndAfter with MockitoSugar {
  //mock storage
  val storage = new JobMetaStorage
  val job_dao = mock[SqoopJobDAO]
  val prop_dao = mock[SqoopJobPropsDAO]
  val job: SqoopJob = SqoopJob(
    id = Some(1),
    job_type = "import",
    job_name = IngestorTest.TEST_JOB_NAME,
    server = IngestorTest.LOCALHOST,
    database = IngestorTest.TEST_DB,
    table_name = IngestorTest.TEST_TABLE,
    enabled = true)
  val e: Either[Failure, SqoopJob] = Right(job)
  val el: Either[Failure, List[SqoopJob]] = Right(List(job))

  //mock prop_dao functions
  val props: List[SqoopJobProp] = List(
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "db.connect.string",
      prop_val = IngestorTest.CONNECTION_STRING,
      prop_class = "SqoopOptions"
    ),
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "hdfs.target.dir",
      prop_val = IngestorTest.TARGET_DIR,
      prop_class = "SqoopOptions"
    ),
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "db.table",
      prop_val = IngestorTest.TEST_TABLE,
      prop_class = "SqoopOptions"
    ),
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "sqoop.tool",
      prop_val = "job",
      prop_class = "schema"
    ),
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "db.split.column",
      prop_val = IngestorTest.SPLIT_BY_COL,
      prop_class = "SqoopOptions"
    ),
    SqoopJobProp(
      job_id = 1,
      job_name = IngestorTest.TEST_JOB_NAME,
      prop_name = "mapreduce.num.mappers",
      prop_val = IngestorTest.NUM_MAPPERS,
      prop_class = "SqoopOptions"
    )
  )

  //mock job_dao function
  when(job_dao.get(IngestorTest.TEST_JOB_NAME)).thenReturn(e)
  when(job_dao.get("foo")).thenReturn(Left(Failure("99999: Job not found", FailureType.DatabaseFailure)))
  when(job_dao.create(job)).thenReturn(e)
  when(job_dao.search(new SqoopJobSearchParameters())).thenReturn(el)

  val ep: Either[Failure, List[SqoopJobProp]] = Right(props)

  when(prop_dao.search(new SqoopJobPropParameters(job_name = Some(IngestorTest.TEST_JOB_NAME)))).thenReturn(ep)

  //mock prop_dao
  storage.set_conn_jobs(job_dao)
  storage.set_conn_props(prop_dao)

  "JobMetaStorage " should {
    val list = asJavaList(List(IngestorTest.TEST_JOB_NAME))

    "read get back a object of type JobData" taggedAs(unitTest) in {
      assert(list === storage.list())
    }

    val job_data = storage.read(IngestorTest.TEST_JOB_NAME)
    job_data shouldBe a[JobData]
    val options = job_data.getSqoopOptions

    "have a job name set to  %s".format(IngestorTest.TEST_JOB_NAME) taggedAs(unitTest) in {
      assert(IngestorTest.TEST_JOB_NAME === options.getJobName)
    }

    "have a connection string set to %s".format(IngestorTest.CONNECTION_STRING) taggedAs(unitTest) in {
      assert(IngestorTest.CONNECTION_STRING === options.getConnectString)
    }
    "have a split by column set to  %s".format(IngestorTest.SPLIT_BY_COL) taggedAs(unitTest) in {
      assert(IngestorTest.SPLIT_BY_COL === options.getSplitByCol)
    }

    "have a target directory set to %s".format(IngestorTest.TARGET_DIR) taggedAs(unitTest) in {
      assert(IngestorTest.TARGET_DIR === options.getTargetDir)
    }

    "have a the number of mappers set to %s".format(IngestorTest.NUM_MAPPERS) taggedAs(unitTest) in {
      assert(IngestorTest.NUM_MAPPERS === options.getNumMappers.toString)
    }

    "check we shouldn't find this job foo"  taggedAs(unitTest) in {
      val pair : Pair[Long, Boolean] = storage.check_if_exists("foo")
      assert(pair._2==false)
    }
  }
}