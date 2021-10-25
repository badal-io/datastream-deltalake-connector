package io.badal.databricks.datastream

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.delta.{DeltaSchemaMigration, TableNameFormatter}
import org.apache.log4j.Logger
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

  val logger = Logger.getLogger(DatastreamIO.getClass)

  def apply(spark: SparkSession,
            datastreamTable: DatastreamTable,
            fileReadConcurrency: Int,
            writeRawCdcTable: Boolean,
            checkpointDir: String,
            schemaEvolutionStrategy: SchemaEvolutionStrategy,
            readFormat: String): DataFrame = {

    /**
      * Generates an intermediate delta table containing raw cdc events
      */
    def logTableStreamFrom(df: DataFrame): DataFrame = {
      val logTableName = TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"/delta/$logTableName"

      logger.info(s"will write raw cdc delta table $logTablePath")

      DeltaSchemaMigration.updateSchemaByPath(logTablePath,
                                              df.schema,
                                              schemaEvolutionStrategy)(spark)

      df.writeStream
        .option(schemaEvolutionStrategy)
        .option("checkpointLocation", s"$checkpointDir$logTablePath")
        .format("delta")
        .outputMode("append")
        .start(logTablePath)

      spark.readStream
        .format("delta")
        .load(logTablePath)
    }

    val inputDf = spark.readStream
      .format(readFormat)
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger", fileReadConcurrency)
      .load(filePaths(datastreamTable.tablePath))

    if (writeRawCdcTable) logTableStreamFrom(inputDf)
    else inputDf
  }

  private def filePaths(path: String): String =
    s"$path/*/*/*/*/*"
}
