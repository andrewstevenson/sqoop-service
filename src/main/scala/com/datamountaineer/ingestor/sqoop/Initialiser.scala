package com.datamountaineer.ingestor.sqoop

import java.sql.{Connection, ResultSet}

import com.datamountaineer.ingestor.models.JobMetaStorage
import org.slf4j.{Logger, LoggerFactory}

object Initialiser {
  val log : Logger = LoggerFactory.getLogger(this.getClass)

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
    var conn: Option[Connection] = null
    try {
      val target_db = new TargetDb(db_type, server, database)
      val query = target_db.get_query().replace("MY_DATABASE", database).replace("MY_SERVER", server)
      conn =  target_db.get_conn()
      conn match {
        case None => log.error("Could not connect to database %s on %s".format(database, server))
        case _ =>
          val stmt = conn.get.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          val rs: ResultSet = stmt.executeQuery(query)
          val storage = new JobMetaStorage
          storage.open()
          while (rs.next()) {
            val input = rs.getString("input")
            //create sqoop options
            val sqoop_options = new IngestSqoop(input, true).build_sqoop_options()
            //call ingestor to create the
            storage.create(sqoop_options)
          }
      }
    }
    finally {
      if (conn != null) conn.get.close()
    }
  }
}