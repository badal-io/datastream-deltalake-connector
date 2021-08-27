package io.badal.databricks.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object DataStreamSchema {
  val PAYLOAD_FIELD = "payload"
  val SOURCE_TIMESTAMP_FIELD = "source_timestamp"
  def payloadSchema(df:DataFrame): StructType = df.schema(PAYLOAD_FIELD).dataType match {
    case s: StructType => s
    case _             =>  throw new Exception("Invalid payload type")
  }
  def payloadFields(df:DataFrame): Array[String] = payloadSchema(df).fieldNames
}
