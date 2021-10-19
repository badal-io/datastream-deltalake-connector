package io.badal.databricks.utils

import io.badal.databricks.datastream.DatastreamTable
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Helper class to read Datastream files and return them as a DataFrame
  * TODO:
  * 1) Add support for JSON
  * 2) Autodiscover tables
  * 3) Turn it into a proper Receiver
  * 4) Add support for maxFileAge
  */
object DatastreamIO {
  def apply(spark: SparkSession,
            datastreamTable: DatastreamTable,
            fileReadConcurrency: Int,
            ourFlag: Boolean): DataFrame = {

    def rawEventStreamFrom(df: DataFrame, table: String): DataFrame = {
      df.writeStream
        .option("mergeSchema", true)
        .format("delta")
        .outputMode("update")
        .start(table)

      spark.readStream
        .format("delta")
        .load(table)
    }

    val inputDf = spark.readStream
      .format("avro")
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger", fileReadConcurrency)
      .load(avroFilePaths(datastreamTable.path))

    if (ourFlag) rawEventStreamFrom(inputDf, datastreamTable.rawTableName)
    else inputDf
  }

  private def avroFilePaths(inputBucket: String): String =
    s"gs://$inputBucket/*/*/*/*/*"
}
