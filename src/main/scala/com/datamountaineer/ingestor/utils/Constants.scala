package com.datamountaineer.ingestor.utils

object Constants {
  val DB_TYPE_KEY : String = "db_type"
  val SERVER_KEY : String = "server"
  val DATABASE_KEY : String = "database"
  val TABLE_KEY : String = "table"
  val SPLIT_BY_KEY : String = "split_by"
  val MAPPERS_KEY : String = "mappers"
  val CHECK_BY_KEY : String = "check_col"
  val LAST_VAL_KEY : String = "last_val"

  val SNAPPY_CODEC : String = "org.apache.hadoop.io.compress.SnappyCodec"

  val SQL_SERVER : String = "sqlserver"
  val MYSQL : String = "mysql"
  val NETEZZA : String = "netezza"
  val TERADATA :  String = "teradata"
  val ORACLE : String = "oracle"

  val SPILT_DELIMITER : String = ":"
}
