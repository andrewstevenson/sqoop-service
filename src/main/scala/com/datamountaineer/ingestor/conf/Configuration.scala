package com.datamountaineer.ingestor.conf

import com.typesafe.config.ConfigFactory

import scala.util.Try

/**
 * Holds service configuration settings.
 */
trait Configuration {

  /**
   * Application config object.
   */
  val config = ConfigFactory.load()

  /** Host name/address to start service on. */
  lazy val serviceHost = Try(config.getString("service.host")).getOrElse("localhost")

  /** Port to start service on. */
  lazy val servicePort = Try(config.getInt("service.port")).getOrElse(8080)

  /** Database host name/address. */
  lazy val dbHost = Try(config.getString("sqoop.db.host")).getOrElse("localhost")

  /** Database host port number. */
  lazy val dbPort = Try(config.getInt("sqoop.db.port")).getOrElse(3306)

  /** Service database name. */
  lazy val dbDatabase = Try(config.getString("sqoop.db.database")).getOrElse("rest")

  /** User name used to access database. */
  lazy val dbUser = Try(config.getString("sqoop.db.username")).toOption.orNull

  /** Password for specified user and database. */
  lazy val dbPassword = Try(config.getString("sqoop.db.password")).toOption.orNull

  lazy val dbDriver = Try(config.getString("sqoop.db.driver")).toOption.orNull

  lazy val dbType = Try(config.getString("sqoop.db.type")).toOption.orNull

  lazy val SqoopTargetDirPreFix = Try(config.getString("sqoop.target_dir_prefix")).toOption.orNull

  lazy val ScrubbedRepoDir = Try(config.getString("repo.scrubbed_root")).getOrElse("/data/scrubbed")

  lazy val MySQLInitialiserSQL = Try(config.getString("initialiser.mysql.queries")).getOrElse("")

  lazy val NetezzaInitialiserSQL = Try(config.getString("initialiser.netezza.queries")).getOrElse("")
}

