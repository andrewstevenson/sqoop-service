package com.datamountaineer.ingestor.models

import java.sql._

import com.datamountaineer.ingestor.conf.Configuration
import com.datamountaineer.ingestor.rest.{Failure, FailureType}

import scala.slick.driver.MySQLDriver.simple.Database.threadLocalSession
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable

class SqoopLogDAO extends Configuration {

  // init Database instance
  private val db = Database.forURL(url = "jdbc:%s://%s:%d/%s".format(dbType, dbHost, dbPort, dbDatabase),
    user = dbUser, password = dbPassword, driver = dbDriver)

  // create tables if not exist
  db.withSession {
    if (MTable.getTables("sqoop_logs").list().isEmpty) {
      SqoopLogs.ddl.create
    }
  }

  /**
   * Saves sqoop log  entity into database.
   *
   * @param table table entity to
   * @return saved table entity
   */
  def create(table: SqoopLog): Either[Failure, SqoopLog] = {
    try {
      val id = db.withSession {
        SqoopLogs returning SqoopLogs.id insert table
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
   * @param id of the log record to update.
   * @param table updated table entity
   * @return updated table entity
   */
  def update(id: Long, table: SqoopLog): Either[Failure, SqoopLog] = {
    try
      db.withSession {
        SqoopLogs.where(_.id === id) update table.copy(id = Some(id)) match {
          case 0 => Left(notFoundError(id.toString))
          case _ => Right(table.copy(id = Some(id)))
        }
      }
    catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Deletes table from database.
   *
   * @param id of the log entry to delete
   * @return deleted table entity
   */
  def delete(id: Long): Either[Failure, SqoopLog] = {
    try {
      db.withTransaction {
        val query = SqoopLogs.where(_.id === id)
        val table = query.run.asInstanceOf[List[SqoopLog]]
        table.size match {
          case 0 =>
            Left(notFoundError(id.toString))
          case _ =>
            query.delete
            Right(table.head)
        }
      }
    } catch {
      case e: SQLException =>
        Left(databaseError(e))
    }
  }

  /**
   * Retrieves specific table from database.
   *
   * @param id of the log entry to retrieve
   * @return  entity with specified findByJobName
   */
  def get(id: Long): Either[Failure, SqoopLog] = {
    try {
      db.withSession {
        SqoopLogs.findByJobId(id).firstOption match {
          case Some(table: SqoopLog) =>
            Right(table)
          case _ =>
            Left(notFoundError(id.toString))
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
  def search(params: SqoopLogParameters): Either[Failure, List[SqoopLog]] = {
    try {
      db.withSession {
        val query = for {
          table <- SqoopLogs if {
          Seq(
            params.job_name.map(table.job_name is _)
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
