package com.datamountaineer.sqoop.models

import java.sql.Date

import scala.slick.driver.MySQLDriver.simple._

case class SqoopJob(id: Option[Long] = None,
                    job_type: String,
                    job_name: String,
                    server: String,
                    database: String,
                    table_name: String)

//object SqoopJobJsonProtocol extends DefaultJsonProtocol {
//  implicit val sqoopJobConfig = jsonFormat7(SqoopJob)
//}

object SqoopJobs extends Table[SqoopJob]("sqoop_jobs") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def job_type = column[String]("job_type")
  def job_name = column[String]("job_name")
  def server = column[String]("server")
  def database = column[String]("database")
  def table_name = column[String]("table_name")

  def idx = index("idx__job_name", job_name, unique = true)
  def * = id.? ~ job_type ~ job_name ~ server ~ database ~ table_name <> (SqoopJob, SqoopJob.unapply _)

  implicit val dateTypeMapper = MappedTypeMapper.base[java.util.Date, java.sql.Date](
  {
    ud => new Date(ud.getTime)
  }, {
    sd => new java.util.Date(sd.getTime)
  })

  val findByJobName = for {
    job_name <- Parameters[String]
    c <- this if c.job_name is job_name
  } yield c
}