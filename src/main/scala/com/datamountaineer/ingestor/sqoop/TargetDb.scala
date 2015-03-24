package com.datamountaineer.ingestor.sqoop

import java.io.IOException
import java.sql.{DriverManager, Connection}

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.util.Try


class TargetDb (conf: Config = null, database_type: String, server: String, database: String) {

  val log = LoggerFactory.getLogger("TargetDb")
  val name = Try(conf.getString("name")).getOrElse(database)
  val host = server
  val db_type = database_type
  val username = Try(conf.getString("username")).getOrElse(get_env_credentials().get._1)
  val password = Try(conf.getString("password")).getOrElse(get_env_credentials().get._1)

  /*
  * WHAT TO REPLACE THIS WITH SLICK!!!!!!
  *
  * */
  val mysql_query = "SELECT DISTINCT " +
    "t.table_name " +
    ", CONCAT_WS(':'" +
    ", 'mysql'" +
    ", @@hostname" +
    ", t.table_schema" +
    ", t.table_name " +
    ", IFNULL(c.column_name, '') " +
    ", '4'" +
    ", IFNULL(c.column_name, '')" +
    ", 0 ) AS input " +
    "FROM information_schema.tables t " +
    "LEFT OUTER JOIN information_schema.columns c " +
    "ON t.table_name = c.table_name " +
    "AND extra LIKE '%auto_increment%'" +
    "WHERE t.table_schema = \"MY_DATABASE\";"

  val netezza_query = "SELECT DISTINCT " +
    "t.tablename AS table_name " +
    ", 'netezza:MY_SERVER:' || t.database || ':' ||  t.tablename || ':' || ISNULL(d.attname, '') || ':8:' || " +
    "ISNULL(d.attname, '') || ':0' AS input " +
    "FROM _v_table t " +
    "LEFT OUTER JOIN _v_table_dist_map d " +
    "ON t.tablename = d.tablename AND t.database = d.database " +
    "LEFT OUTER JOIN _v_relation_column c " +
    "ON d.database = c.database AND t.objid = c.objid AND c.format_type = 'BIGINT'"

  def get_env_credentials() : Option[Pair[String, String]] = {
    //check environment variables
    val env_pass = db_type.toUpperCase + "_" + host.toUpperCase + "_" + name.toUpperCase + "_PASS"
    val env_usr = db_type.toUpperCase + "_" + host.toUpperCase + "_" + name.toUpperCase + "_USER"
    val pass: Option[String] = Some(System.getenv(env_pass))
    val user: Option[String] = Some(System.getenv(env_usr))

    if (pass.get == null || user.get == null) {
      log.error("Did not find database called %s in application.conf or environment variables %s and %s".format(name, env_usr, env_pass))
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
      case "netezza" =>
        classOf[org.netezza.Driver].newInstance()
        val conn = DriverManager.getConnection("jdbc:netezza://" + host + ":5480/" + name,
          username, password)
        Some(conn)
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