package com.datamountaineer.ingestor.sqoop

import java.io.IOException
import java.sql.{Connection, DriverManager, ResultSet}

import com.datamountaineer.ingestor.models.JobMetaStorage
import com.datamountaineer.ingestor.conf.Configuration
import org.slf4j.LoggerFactory

import scala.util.Try

object Initialiser  extends Configuration {
  val log = LoggerFactory.getLogger("initialiser")
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

  def main(args: Array[String]) {
    if (args == null || args.length < 3) {
      System.out.println( """
                            |Usage: <initialiser db_type server database>
                          """.stripMargin)
      System.exit(0)
    }

    val db_type = args(0).toString
    val server = args(1).toString
    val database = args(2).toString
    initialise(db_type, server, database)
  }

  def initialise(db_type: String, server: String, database: String) = {
    var conn: Connection = null
    try {
      conn = get_conn(db_type, server, database)
      val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val query = get_query(db_type).replace("MY_DATABASE", database).replace("MY_SERVER", server)
      val rs: ResultSet = stmt.executeQuery(query)
      while (rs.next()) {
        val input = rs.getString("input")
        //create sqoop options
        val sqoop_options = new IngestSqoop(input, true).build_sqoop_options()
        //call ingestor to create the
        val storage = new JobMetaStorage
        storage.open()
        storage.create(sqoop_options)
      }
    }
    finally {
      conn.close()
    }
  }

  /**
   * Return query for a given db_type
   * @param db_type Type of database
   * */
  def get_query(db_type: String) : String = {
    db_type.toLowerCase match {
      case "mysql" =>
        mysql_query
      case "netezza" =>
        netezza_query
    }
  }

  /**
  * Returns a connection for given db type
  * @param db_type Type of database e.g. mysql, netezza
   * @param server Server hosting the database
   * @param database Database database to connect to
  * */
  def get_conn(db_type: String, server: String, database: String) : Connection = {
    db_type.toLowerCase match {
      case "mysql" =>
        val conn_str = "jdbc:mysql://" + server + ":3306/" + database
        val mysql_username = Try(config.getString(db_type + "_" + server + "_" + database + "_db." + "username"))
          .getOrElse(System.getenv((db_type + "_" + server + "_" + database + "_USER").toUpperCase()))

        if (mysql_username.equals("")) log.error("Unable to find user name in config files for %s".format(server))

        val mysql_password = Try(config.getString(db_type + "_" + server + "_" + database + "_db." + "password"))
          .getOrElse(System.getenv((db_type + "_" + server + "_" + database + "_PASS").toUpperCase()))

        if (mysql_password.equals("")) log.error("Unable to find password in config files for %s".format(server))

        //Class.forName("com.mysql.jdbc.Driver").newInstance
        classOf[com.mysql.jdbc.Driver].newInstance()
        try {
          val conn = DriverManager.getConnection(conn_str, mysql_username, mysql_password)
          conn
        } catch {
          case e: Exception  =>
            log.error(e.getMessage, new IOException)
            throw e
        }
      case "netezza" =>
        classOf[org.netezza.Driver].newInstance()
        val username = Try(config.getString(db_type + "_" + server + "_" + database + "." + "username"))
          .getOrElse(System.getenv((db_type + "_" + server + "_" + database + "_USER").toUpperCase()))

        if (username.equals("")) log.error("Unable to find user name in config files for %s".format(server))


        val password = Try(config.getString(db_type + "_" + server + "_" + database + "." + "password"))
          .getOrElse(System.getenv((db_type + "_" + server + "_" + database + "_PASS").toUpperCase()))

        if (password.equals("")) log.error("Unable to find user name in config files for %s".format(server))

        val conn = DriverManager.getConnection("jdbc:netezza://" + server + ":5480/" + database,
          username,password)
        conn
    }
  }
}