package com.datamountaineer.sqoop.models

import scala.slick.driver.MySQLDriver.simple._


case class SqoopJobProp(id: Option[Long] = None,
                        job_id: Long,
                        job_name: String,
                        prop_name: String,
                        prop_val: String,
                        prop_class: String)

//object SqoopJobPropsJsonProtocol extends DefaultJsonProtocol {
//  implicit val sqoopJobConfig = jsonFormat6(SqoopJobProp)
//}

/**
 * Mapped sqoop table table object.
 */
object SqoopJobProps extends Table[SqoopJobProp]("sqoop_job_props") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def job_name = column[String]("job_name")
  def prop_name = column[String]("prop_name")
  def prop_val = column[String]("prop_val")
  def prop_class = column[String]("prop_class")
  def job_id = column[Long]("job_id")
  def job = foreignKey("Job_FK", job_id, SqoopJobs)(_.id)

  def idx = index("idx__job_name", job_name, unique = false)
  def * = id.? ~ job_id ~ job_name ~ prop_name ~ prop_val ~ prop_class <> (SqoopJobProp, SqoopJobProp.unapply _)

  val findByJobName = for {
    job_name <- Parameters[String]
    c <- this if c.job_name is job_name
  } yield c
}