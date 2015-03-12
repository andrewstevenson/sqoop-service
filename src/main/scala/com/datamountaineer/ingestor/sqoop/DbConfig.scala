package com.datamountaineer.ingestor.sqoop

import com.typesafe.config.Config
import scala.util.Try

class DbConfig (conf: Config, db: String, host: String) {
    val name = conf.getString("name")
    private val username = conf.getString("username")
    private val password = conf.getString("password")
    val db_type = db
    val server = host

    /**
     * Return a pair containing the username and password
     * either from the application.conf
     * */
    def get_credentials() : Pair[String, String] = {
      Pair(Try(username).getOrElse(""),  Try(password).getOrElse(""))
    }
}
