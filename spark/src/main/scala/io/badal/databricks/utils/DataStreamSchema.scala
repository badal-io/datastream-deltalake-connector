package io.badal.databricks.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object DataStreamSchema {
  val PayloadField = "payload"

  def registerIfNotExists(spark: SparkSession, database: String): Unit =
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")

  def payloadSchema(df: DataFrame): StructType =
    df.schema(PayloadField).dataType match {
      case s: StructType => s
      case _             => throw new Exception("Invalid payload type")
    }

  def payloadFields(df: DataFrame): Array[String] = payloadSchema(df).fieldNames

}
