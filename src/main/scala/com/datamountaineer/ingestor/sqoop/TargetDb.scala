package com.datamountaineer.ingestor.sqoop

import java.io.IOException
import java.sql.{Connection, DriverManager}

import com.datamountaineer.ingestor.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

class TargetDb (database_type: String, server: String, database: String) extends Configuration {

  val log : Logger = LoggerFactory.getLogger(this.getClass)
  val name = database
  val host = server
  val db_type = database_type

  val username = Try(config.getString(database_type.toLowerCase + "." + serviceHost.toLowerCase + "." +
    database.toLowerCase + ".username")).getOrElse(get_env_credentials().get._1)
  val password = Try(config.getString(database_type.toLowerCase + "." + serviceHost.toLowerCase + "." +
    database.toLowerCase + ".password")).getOrElse(get_env_credentials().get._2)
  val mysql_query =  Try(config.getString("initialiser.query.mysql")).getOrElse("")
  val netezza_query =  Try(config.getString("initialiser.query.netezza")).getOrElse("")

  def get_env_credentials() : Option[Pair[String, String]] = {
    //check environment variables
    val env_pass = db_type.toUpperCase + "_" + host.toUpperCase + "_" + name.toUpperCase + "_PASS"
    val env_usr = db_type.toUpperCase + "_" + host.toUpperCase + "_" + name.toUpperCase + "_USER"
    val pass: Option[String] = Some(System.getenv(env_pass))
    val user: Option[String] = Some(System.getenv(env_usr))

    if (pass.get == null || user.get == null) {
      log.error("Did not find database called %s in application.conf or environment variables %s and %s"
        .format(name, env_usr, env_pass))
      System.exit(-1)
      None
    }
    else {
      log.info("Found credentials in environment variables.")
      Some(Pair(user.get, pass.get))
    }
  }


  def get_conn() : Option[Connection] = {
    db_type.toLowerCase match {
      case "mysql" =>
        val conn_str = "jdbc:mysql://" + host + ":3306/" + name
        log.info("Attempting to connect to %s with connection string %s".format(name, conn_str))
        classOf[com.mysql.jdbc.Driver].newInstance()
        try {
          val conn = DriverManager.getConnection(conn_str, username, password)
          Some(conn)
        } catch {
          case e: Exception =>
            log.error(e.getMessage, new IOException)
            throw e
        }
//      case "netezza" =>
//        classOf[org.netezza.Driver].newInstance()
//        val conn = DriverManager.getConnection("jdbc:netezza://" + host + ":5480/" + name,
//          username, password)
//        Some(conn)
    }
  }

  /**
   * Return query for a given db_type
   * */
  def get_query() : String = {
    db_type.toLowerCase match {
      case "mysql" =>
        mysql_query
      case "netezza" =>
        netezza_query
    }
  }
}