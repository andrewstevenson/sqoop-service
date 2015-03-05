package com.datamountaineer.sqoop.sqoop

import java.sql.{Connection, DriverManager, ResultSet}

import com.datamountaineer.sqoop.models.JobMetaStorage
import org.slf4j.LoggerFactory

object initialiser {
  val log = LoggerFactory.getLogger("initialiser")
  //mysql:localhost:my_sqoop_test
  val job_name_format = "%s:%s:%s:%s"
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

    var conn: Connection = null

    try {
      val db_type = args(0).toString
      val server = args(1).toString
      val database = args(2).toString

      conn = get_conn(db_type, server, database)
      val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val query = get_query(db_type).replace("MY_DATABASE", database).replace("MY_SERVER", server)
      val rs: ResultSet = stmt.executeQuery(query)
      while (rs.next()) {
        val table = rs.getString("table_name")
        val input = rs.getString("input")
        //create sqoop options
        val sqoop_options = new ingestSqoop(input, true).build_sqoop_options()
        //call ingestor to create the
        val storage = new JobMetaStorage
        storage.create(job_name_format.format(db_type, server, database, table), sqoop_options)
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
        val mysql_username = "sqoop"
        val mysql_password = "sqoop"
        val conn_str = "jdbc:mysql://" + server + ":3306/" + database
        //Class.forName("com.mysql.jdbc.Driver").newInstance
        classOf[com.mysql.jdbc.Driver].newInstance()
        try {
          val conn = DriverManager.getConnection(conn_str, mysql_username, mysql_password)
          conn
        } catch {
          case e: Exception  => {
            println("ERROR: Driver not available: " + e.getMessage)
            throw e
          }
        }
      case "netezza" =>
        classOf[org.netezza.Driver].newInstance()
        val conn = DriverManager.getConnection("jdbc:netezza://" + server + ":5480/" + database,
          System.getenv("NETEZZA_USER"),
          System.getenv("NETEZZA_PASS"))
        conn
    }
  }
}