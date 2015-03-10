package com.datamountaineer.ingestor.models


import com.datamountaineer.ingestor.{IngestorTest, IngestorTestTrait}
import com.datamountaineer.ingestor.rest.Failure
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class JobMetaStorageTest extends IngestorTest with BeforeAndAfter with MockitoSugar {
  val storage = new JobMetaStorage

  test("Storage should build job name from SqoopOptions connection string") {
    assert(storage.get_job_name(options) === test_job_name)
  }

  test("") {
    val job_dao = mock[SqoopJobDAO]
    val prop_dao = mock[SqoopJobPropsDAO]
    val job: SqoopJob = SqoopJob(job_type = "import",
      job_name = test_job_name,
      server = LOCALHOST,
      database = TEST_DB,
      table_name = TEST_TABLE,
      enabled = true)
    val e : Either[Failure, SqoopJob] = Right(job)
    val el : Either[Failure, List[SqoopJob]] = Right(List(job))
    //mock job_dao
    when(job_dao.get(test_job_name)).thenReturn(e)
    when(job_dao.create(job)).thenReturn(e)
    when(job_dao.search(AnyRef[SqoopJobPropParameters])).thenReturn(el)

    //mock prop_dao
    storage.set_conn_jobs(job_dao)
    storage.set_conn_props(prop_dao)

    assert(storage.list() === el)
  }
}