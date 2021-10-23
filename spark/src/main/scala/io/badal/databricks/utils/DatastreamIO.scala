package io.badal.databricks.utils

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.datastream.DatastreamTable
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import SchemaEvolutionStrategy._
import org.apache.log4j.Logger

/**
  * Helper class to read Datastream files and return them as a DataFrame
  * TODO:
  * 1) Add support for JSON
  * 2) Autodiscover tables
  * 3) Turn it into a proper Receiver
  * 4) Add support for maxFileAge
  */
object DatastreamIO {

  val logger = Logger.getLogger(DatastreamIO.getClass)

  def apply(datastreamTable: DatastreamTable,
            fileReadConcurrency: Int,
            writeRawCdcTable: Boolean,
            checkpointDir: String,
            schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DataFrame = {

    /**
      * Generates an intermediate delta table containing raw cdc events
      */
    def logTableStreamFrom(df: DataFrame)(
        implicit sparkSession: SparkSession): DataFrame = {
      val logTableName = TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"/delta/$logTableName"

      logger.info(s"will write raw cdc delta table $logTablePath")

      DeltaSchemaMigration.updateSchemaByPath(logTablePath,
                                              df.schema,
                                              schemaEvolutionStrategy)(spark)

      df.writeStream
        .option(schemaEvolutionStrategy)
        .option("checkpointLocation", s"dbfs:/$checkpointDir$logTablePath")
        .format("delta")
        .outputMode("append")
        .start(logTablePath)

      spark.readStream
        .format("delta")
        .load(logTablePath)
    }

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
