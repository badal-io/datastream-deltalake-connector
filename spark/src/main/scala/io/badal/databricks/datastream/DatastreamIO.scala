package io.badal.databricks.datastream

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.delta.MergeQueries.log
import io.badal.databricks.delta.{DeltaSchemaMigration, TableMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Helper class to read Datastream files and return them as a DataFrame
  * TODO:
  * 1) Turn it into a proper Receiver
  * 2) Add support for maxFileAge
  */
object DatastreamIO {

  val logger = Logger.getLogger(DatastreamIO.getClass)

  def readStreamFor(datastreamTable: DatastreamTable,
                    jobConf: DatastreamDeltaConf,
                    spark: SparkSession): Option[(DataFrame, TableMetadata)] = {

    /**
      * Generates an intermediate delta table containing raw cdc events
      */
    def logTableStreamFrom(df: DataFrame,
                           tableMetadata: TableMetadata): DataFrame = {

      val logTableName = tableMetadata.table.fullLogTableName
      val logTablePath = s"${jobConf.deltalake.tablePath}/$logTableName"

      logger.info(s"will write raw cdc delta table ${tableMetadata.table}")

      // Create the table if it doesn't exist
      DeltaSchemaMigration.createOrUpdateSchema(
        jobConf.deltalake.tablePath,
        tableMetadata,
        jobConf.deltalake.schemaEvolution,
        spark
      )

      df.writeStream
        .option(jobConf.deltalake.schemaEvolution)
        .option("checkpointLocation", s"${jobConf.checkpointDir}/$logTableName")
        .format("delta")
        .outputMode("append")
        .start(logTablePath)

      spark.readStream
        .format("delta")
        .load(logTablePath)
    }

    val paths = filePaths(datastreamTable.tablePath)

    logger.info(
      s"defining stream for Datastream table source located at $paths")

    val streamingInputDf = spark.readStream
      .format(jobConf.datastream.readFormat.value)
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger",
              jobConf.datastream.fileReadConcurrency.value)
      .load(filePaths(datastreamTable.tablePath))

    TableMetadata.fromDf(streamingInputDf) match {
      case Some(tableMetadata) =>
        if (jobConf.generateLogTable) {
          Some(logTableStreamFrom(streamingInputDf, tableMetadata),
               tableMetadata)
        } else {
          Some(streamingInputDf, tableMetadata)
        }
      case None =>
        log.error(
          s"empty folder ${datastreamTable.tablePath} " +
            s" for table ${datastreamTable} - skipping log table creation")
        None
    }
  }

  private def filePaths(path: String): String =
    s"$path/*/*/*/*/*"
}
