package io.badal.databricks.datastream

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.delta.DeltaSchemaMigration
import io.badal.databricks.utils.TableNameFormatter
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
                    spark: SparkSession): DataFrame = {

    /**
      * Generates an intermediate delta table containing raw cdc events
      */
    def logTableStreamFrom(df: DataFrame): DataFrame = {
      val logTableName = TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"${jobConf.deltalake.tablePath}/$logTableName"

      logger.info(s"will write raw cdc delta table $logTableName")

      DeltaSchemaMigration.createOrUpdateSchema(
        logTableName,
        logTablePath,
        df.schema,
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

    val inputDf = spark.readStream
      .format(jobConf.datastream.readFormat.value)
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger",
              jobConf.datastream.fileReadConcurrency.value)
      .load(filePaths(datastreamTable.tablePath))

    if (jobConf.generateLogTable) logTableStreamFrom(inputDf)
    else inputDf
  }

  private def filePaths(path: String): String =
    s"$path/*/*/*/*/*"
}
