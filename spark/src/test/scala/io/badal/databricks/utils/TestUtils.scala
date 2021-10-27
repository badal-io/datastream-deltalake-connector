package io.badal.databricks.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtils {
  def readJsonRecords(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("multiline", "true")
      .json(getClass.getResource(path).toString)
}
