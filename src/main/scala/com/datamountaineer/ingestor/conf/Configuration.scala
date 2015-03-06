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
  lazy val dbHost = Try(config.getString("db.host")).getOrElse("localhost")

  /** Database host port number. */
  lazy val dbPort = Try(config.getInt("db.port")).getOrElse(3306)

  /** Service database name. */
  lazy val dbDatabase = Try(config.getString("db.database")).getOrElse("rest")

  /** User name used to access database. */
  lazy val dbUser = Try(config.getString("db.user")).toOption.orNull

  /** Password for specified user and database. */
  lazy val dbPassword = Try(config.getString("db.password")).toOption.orNull

  lazy val dbDriver = Try(config.getString("db.driver")).toOption.orNull

  lazy val dbType = Try(config.getString("db.type")).toOption.orNull

  lazy val SqoopTargetDirPreFix = Try(config.getString("sqoop.target_dir_prefix")).toOption.orNull
}

