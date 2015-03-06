package com.datamountaineer.ingestor.models

case class SqoopJobSearchParameters( job_name: Option[String] = None,
                                  table: Option[String] = None,
                                  database: Option[String] = None,
                                  server: Option[String] = None)

case class SqoopJobPropParameters( job_name: Option[String] = None, prop_name: Option[String] = None)