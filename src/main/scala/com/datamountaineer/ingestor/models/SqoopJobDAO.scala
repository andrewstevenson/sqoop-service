package com.datamountaineer.ingestor.models

import java.sql._

import com.datamountaineer.ingestor.conf.Configuration
import com.datamountaineer.ingestor.rest.{Failure, FailureType}

import scala.slick.driver.MySQLDriver.simple.Database.threadLocalSession
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable

class SqoopJobDAO extends Configuration {

  // init Database instance
  private val db = Database.forURL(url = "jdbc:%s://%s:%d/%s".format(dbType, dbHost, dbPort, dbDatabase),
    user = dbUser, password = dbPassword, driver = dbDriver)

  // create tables if not exist
  db.withSession {
    if (MTable.getTables("sqoop_jobs").list().isEmpty) {
      SqoopJobs.ddl.create
    }
  }

  /**
   * Saves sqoop table conf entity into database.
   *
   * @param table table entity to
   * @return saved table entity
   */
  def create(table: SqoopJob): Either[Failure, SqoopJob] = {
    try {
      val id = db.withSession {
        SqoopJobs returning SqoopJobs.id insert table
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
  def update(job_name: String, table: SqoopJob): Either[Failure, SqoopJob] = {
    try
      db.withSession {
        SqoopJobs.where(_.job_name === job_name) update table.copy(job_name = job_name) match {
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
   * Deletes table from database.
   *
   * @param job_name of the table to delete
   * @return deleted table entity
   */
  def delete(job_name: String): Either[Failure, SqoopJob] = {
    try {
      db.withTransaction {
        val query = SqoopJobs.where(_.job_name === job_name)
        val table = query.run.asInstanceOf[List[SqoopJob]]
        table.size match {
          case 0 =>
            Left(notFoundError(job_name))
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
   * @param job_name of the table to retrieve
   * @return  entity with specified findByJobName
   */
  def get(job_name: String): Either[Failure, SqoopJob] = {
    try {
      db.withSession {
        SqoopJobs.findByJobName(job_name).firstOption match {
          case Some(table: SqoopJob) =>
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
  def search(params: SqoopJobSearchParameters): Either[Failure, List[SqoopJob]] = {
    try {
      db.withSession {
        val query = for {
          table <- SqoopJobs if {
          Seq(
            params.job_name.map(table.job_name is _),
            params.server.map(table.server is _),
            params.database.map(table.database is _),
            params.table.map(table.table_name is _),
            params.table.map(table.enabled is _)
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