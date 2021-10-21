package io.badal.databricks.utils

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.datastream.DatastreamTable
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import SchemaEvolutionStrategy._

/**
  * Helper class to read Datastream files and return them as a DataFrame
  * TODO:
  * 1) Add support for JSON
  * 2) Autodiscover tables
  * 3) Turn it into a proper Receiver
  * 4) Add support for maxFileAge
  */
object DatastreamIO {
  def apply(datastreamTable: DatastreamTable,
            fileReadConcurrency: Int,
            writeRawCdcTable: Boolean,
            checkpointDir: String,
            schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DataFrame = {

    def logTableStreamFrom(df: DataFrame)(
        implicit sparkSession: SparkSession): DataFrame = {
      val logTableName = TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"/delta/$logTableName"

      println(s"will write raw cdc delta table $logTablePath")

      val emptyDF =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)

      emptyDF.write
        .option(schemaEvolutionStrategy)
        .format("delta")
        .mode(SaveMode.Append)
        .save(logTablePath)

      df.writeStream
        .option(schemaEvolutionStrategy)
//        .option("checkpointLocation", s"/$checkpointDir$logTablePath")
        .format("delta")
        .outputMode("append")
        .start(logTablePath)

      spark.readStream
        .format("delta")
        .load(logTablePath)
    }

    println(s"loading ${avroFilePaths(datastreamTable.path)}")

    val inputDf = spark.readStream
      .format("avro")
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger", fileReadConcurrency)
      .load(avroFilePaths(datastreamTable.path))

    if (writeRawCdcTable) logTableStreamFrom(inputDf)
    else inputDf
  }

  private def avroFilePaths(inputBucket: String): String =
    s"gs://$inputBucket/*/*/*/*/*"
}
