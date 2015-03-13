package com.datamountaineer.ingestor.sqoop

import com.datamountaineer.ingestor.conf.Configuration
import com.datamountaineer.ingestor.models.JobMetaStorage

import java.sql.{Connection, ResultSet}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

object Initialiser  extends Configuration {
  val log = LoggerFactory.getLogger("Initialiser")

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
      val target_db = get_db_conf(db_type=db_type, server=server, database = database)
      val query = target_db.get_query().replace("MY_DATABASE", database).replace("MY_SERVER", server)
      conn =  target_db.get_conn()
      conn match {
        case None => log.error("Could not connect to database %s on %s".format(database, server))
        case _ => {
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
    }
    finally {
      if (conn != null) conn.get.close()
    }
  }


  def get_db_conf(db_type: String, database: String, server: String) : TargetDb = {
    val conf_key = "%s.%s.dbs".format(db_type, server)
    try {
      val conf: mutable.Buffer[TargetDb] = config.getConfigList(conf_key) map (new TargetDb(_, db_type, server, database))
      val db_list = for (db <- conf if db.name.equals(database)) yield db.asInstanceOf[TargetDb]
      if (db_list.size > 1) log.warn("Found more than one database called %s configured.".format(database))
      if (db_list.size == 0) {
        log.warn("Unable to get credentials for %s from application.conf.".format(database))
        new TargetDb(database_type=db_type, server=server, database=database)
      } else {
        db_list.head
      }
    } catch {
      case ce : com.typesafe.config.ConfigException => {
        new TargetDb(database_type=db_type, server=server, database=database)
      }
    }
  }
}