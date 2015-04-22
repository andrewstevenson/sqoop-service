package com.datamountaineer.ingestor.models

import java.sql.Date

import scala.slick.driver.MySQLDriver.simple._

case class SqoopLog(id: Option[Long] = None,
                    job_name: String,
                    sqoop_start_time: String,
                    sqoop_finish_time: Option[String] = None,
                    sqoop_status: Option[String] = None,
                    sqoop_rec_count: Option[Long] = None,
                    kite_start_time: Option[String] = None,
                    kite_finish_time: Option[String] = None,
                    kite_status: Option[String] = None,
                    kite_rec_count: Option[Long] = None)

//object SqoopJobJsonProtocol extends DefaultJsonProtocol {
//  implicit val sqoopJobConfig = jsonFormat7(SqoopJob)
//}

object SqoopLogs extends Table[SqoopLog]("sqoop_logs") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def job_name = column[String]("job_name")
  def sqoop_start_time = column[String]("sqoop_start_time")
  def sqoop_finish_time = column[Option[String]]("sqoop_finish_time")
  def sqoop_status = column[Option[String]]("sqoop_status")
  def sqoop_rec_count = column[Option[Long]]("sqoop_rec_count")

  def kite_start_time = column[Option[String]]("kite_start_time")
  def kite_finish_time = column[Option[String]]("kite_finish_time")
  def kite_status = column[Option[String]]("kite_status")
  def kite_rec_count = column[Option[Long]]("kite_rec_count")

  def * = id.? ~ job_name ~ sqoop_start_time ~ sqoop_finish_time ~ sqoop_status ~ sqoop_rec_count ~ kite_start_time ~ kite_finish_time ~ kite_status ~ kite_rec_count <> (SqoopLog, SqoopLog.unapply _)

  implicit val dateTypeMapper = MappedTypeMapper.base[java.util.Date, java.sql.Date](
  {
    ud => new Date(ud.getTime)
  }, {
    sd => new java.util.Date(sd.getTime)
  })

  val findByJobId = for {
    id <- Parameters[Long]
    c <- this if c.id is id
  } yield c
}