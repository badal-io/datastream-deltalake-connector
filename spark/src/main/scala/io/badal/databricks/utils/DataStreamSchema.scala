package io.badal.databricks.utils

import io.badal.databricks.datastream.DatastreamTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object DataStreamSchema {
  val PayloadField = "payload"
  val SourceTimestampField = "source_timestamp"

  def registerIfNotExists(spark: SparkSession, datastreamTable: DatastreamTable): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${datastreamTable.database}")
    spark.sql(s"CREATE TABLE IF NOT EXISTS ${datastreamTable.database}.")

  }

  def payloadSchema(df: DataFrame): StructType =
    df.schema(PayloadField).dataType match {
      case s: StructType => s
      case _             => throw new Exception("Invalid payload type")
    }

  def payloadFields(df: DataFrame): Array[String] = payloadSchema(df).fieldNames

}
