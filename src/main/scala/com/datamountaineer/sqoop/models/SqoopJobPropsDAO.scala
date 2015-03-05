package com.datamountaineer.sqoop.models

import java.sql._

import com.datamountaineer.sqoop.conf.Configuration
import com.datamountaineer.sqoop.rest.{Failure, FailureType}

import scala.slick.jdbc.meta.MTable
import scala.slick.session.Database

import scala.slick.driver.MySQLDriver.simple.Database.threadLocalSession
import scala.slick.driver.MySQLDriver.simple._

class SqoopJobPropsDAO extends Configuration {

  // init Database instance
  private val db = Database.forURL(url = "jdbc:%s://%s:%d/%s".format(dbType, dbHost, dbPort, dbDatabase),
    user = dbUser, password = dbPassword, driver = dbDriver)

  // create tables if not exist
  db.withSession {
    if (MTable.getTables("sqoop_job_props").list().isEmpty) {
      SqoopJobProps.ddl.create
    }
  }

  /**
   * Saves sqoop table conf entity into database.
   *
   * @param table table entity to
   * @return saved table entity
   */
  def create(table: SqoopJobProp): Either[Failure, SqoopJobProp] = {
    try {
      val id = db.withSession {
        SqoopJobProps returning SqoopJobProps.id insert table
      }
      Right(table.copy(id = Some(id)))
    } catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Updates table entity with specified one.
   *
   * @param job_name of the table to update.
   * @param table updated table entity
   * @return updated table entity
   */
  def update(job_name: String, table: SqoopJobProp): Either[Failure, SqoopJobProp] = {
    try
      db.withSession {
        SqoopJobProps.where(_.job_name === job_name) update table.copy(job_name = job_name) match {
          case 0 => Left(notFoundError(job_name))
          case _ => Right(table.copy(job_name = job_name))
        }
      }
    catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Deletes job_name props from database.
   *
   * @param job_name of the table to delete
   * @return deleted table entity
   */
  def delete(job_name: String): Int = {
    try {
      db.withTransaction {
        val query = SqoopJobProps.where(_.job_name === job_name)
            query.delete
      }
    }
  }

  /**
   * Deletes prop_name from database.
   *
   * @param job_name of the job to delete
   * @param prop_name of the property to delete
   * @return deleted table entity
   */
  def delete(job_name: String, prop_name: String): Int = {
    try {
      db.withTransaction {
        val query = SqoopJobProps.where(p => p.job_name === job_name && p.prop_name === prop_name)
        query.delete
      }
    }
  }

  /**
   * Retrieves specific table from database.
   *
   * @param job_name of the table to retrieve
   * @return  entity with specified findByJobName
   */
  def get(job_name: String): Either[Failure, SqoopJobProp] = {
    try {
      db.withSession {
        SqoopJobProps.findByJobName(job_name).firstOption match {
          case Some(table: SqoopJobProp) =>
            Right(table)
          case _ =>
            Left(notFoundError(job_name))
        }
      }
    } catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Retrieves list of jobs with specified parameters from database.
   *
   * @param params search parameters
   * @return list of tables that match given parameters
   */
  def search(params: SqoopJobPropParameters): Either[Failure, List[SqoopJobProp]] = {
    try {
      db.withSession {
        val query = for {
          table <- SqoopJobProps if {
          Seq(
            params.job_name.map(table.job_name is _),
            params.prop_name.map(table.prop_name is _)
          ).flatten match {
            case Nil => ConstColumn.TRUE
            case seq => seq.reduce(_ && _)
          }
        }
        } yield table
        Right(query.run.toList)
      }
    } catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Produce database error description.
   *
   * @param e SQL Exception
   * @return database error description
   */
  protected def databaseError(e: SQLException) =
    Failure("%d: %s".format(e.getErrorCode, e.getMessage), FailureType.DatabaseFailure)

  /**
   * Produce customer not found error description.
   *
   * @param job_name of the job
   * @return not found error description
   */
  protected def notFoundError(job_name: String) =
    Failure("Job with name=%s does not exist".format(job_name), FailureType.NotFound)
}