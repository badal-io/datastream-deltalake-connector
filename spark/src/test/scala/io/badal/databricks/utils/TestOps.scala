package io.badal.databricks.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestOps {
  def readDeltaTable(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read.format("delta").load(path)

  def readDeltaTableByName(table: String)(
      implicit spark: SparkSession): DataFrame =
    spark.table(table)

}
