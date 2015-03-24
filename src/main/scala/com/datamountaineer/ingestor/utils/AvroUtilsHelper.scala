package com.datamountaineer.ingestor.utils

import java.util

import com.cloudera.sqoop.SqoopOptions
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.sqoop.manager.{DirectNetezzaManager, NetezzaManager, DirectMySQLManager, MySQLManager}
import org.apache.sqoop.orm.AvroSchemaGenerator
import org.codehaus.jackson.node.NullNode
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._


/**
 * Created by andrew on 24/03/15.
 */
object AvroUtilsHelper {
  val log : Logger = LoggerFactory.getLogger(this.getClass)

  /**
  Extract the none null type for a UNION type
  @param schema A union schema
  @return The none null schema type from the union
    */
  def get_non_null(schema : Schema) = {
    if (schema.getType().equals(Schema.Type.UNION)) {
      val schemas: util.List[Schema] = schema.getTypes
      if (schemas.size() == 2) {
        if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
          schemas.get(1)
        } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
          schemas.get(0)
        } else {
          schema
        }
      } else {
        schema
      }
    } else {
      schema
    }
  }

  /**
   * Generate an avro schema for a database.
   *
   * Uses Sqoop's AvroGenerator but switchs the unions to be [null, type]
   * and sets default to null
   *
   * @param db_type Type of database
   * @param options The SqoopOptions the AvroGenerator will use.
   * @return A avro schema representing the sqoop table
   * */
  def get_avro_schema(db_type : String, options: SqoopOptions) : Schema = {
    val table_name = options.getTableName
    val conn = db_type match {
      case "mysql" => {
        if (options.isDirect) new MySQLManager(options) else new DirectMySQLManager(options)
      }
      case "netezza" => {
        if (options.isDirect) new NetezzaManager(options) else new DirectNetezzaManager(options)
      }
    }
    val avro = new AvroSchemaGenerator(options, conn, options.getTableName)
    log.info("Connecting to %s to generate Avro schema for %s".format(options.getConnectString, table_name))
    val schema = avro.generate()

    //now add default value (set to null for sqoop)
    val fields = schema.getFields
    var new_fields = new ListBuffer[Field]()

    fields.foreach(field => {
      //switch union to be [null, type]. What do we set as the default otherwise, this way can set null
      val new_types : util.List[Schema] = List(Schema.create(Schema.Type.NULL), Schema.create(get_non_null(field.schema()).getType))
      val new_field : Field = new Field(field.name(), Schema.createUnion(new_types), null, NullNode.getInstance())
      new_fields += new_field
    })

    //recreate sqoop attributes
    val doc : String = "Sqoop import of " + table_name
    val new_schema : Schema = Schema.createRecord(table_name, doc, null, false)
    new_schema.setFields(new_fields)
    new_schema
  }
}
