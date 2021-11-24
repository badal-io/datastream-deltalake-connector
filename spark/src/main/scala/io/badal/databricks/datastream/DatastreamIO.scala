package io.badal.databricks.datastream

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.delta.{DeltaSchemaMigration, TableMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Helper class to read Datastream files and return them as a DataFrame
  * TODO:
  * 1) Turn it into a proper Receiver
  * 2) Add support for maxFileAge
  */
object DatastreamIO {

  val logger = Logger.getLogger(DatastreamIO.getClass)

  def readTableMetadata(
      datastreamTable: DatastreamTable,
      jobConf: DatastreamDeltaConf,
      spark: SparkSession
  ): Try[TableMetadata] = {
    val paths = filePaths(datastreamTable.tablePath)

    logger.info(
      s"reading table metadata for Datastream table source located at $paths")

    val inputDf = spark.read
      .format(jobConf.datastream.readFormat.value)
      .option("ignoreExtension", true)
      .load(filePaths(datastreamTable.tablePath))
      .limit(1)

    TableMetadata.fromDf(inputDf, jobConf.deltalake.database.map(_.value)) match {
      case Some(tableMetadata: TableMetadata) =>
        Success(tableMetadata)
      case None =>
        Failure(new RuntimeException(
          s"Failed to retrieve table metadata for table ${datastreamTable.table} at $paths"))
    }
  }

  def readStreamFor(datastreamTable: DatastreamTable,
                    tableMetadata: TableMetadata,
                    jobConf: DatastreamDeltaConf,
                    spark: SparkSession): DataFrame = {

    val paths = filePaths(datastreamTable.tablePath)

    /**
      * Generates an intermediate delta table containing raw cdc events
      */
    def logTableStreamFrom(df: DataFrame): DataFrame = {

      val logTableName = tableMetadata.table.fullLogTableName
      val logTablePath = s"${jobConf.deltalake.tablePath}/$logTableName"

      logger.info(s"will write raw cdc delta table ${tableMetadata.table}")

      // Create the table if it doesn't exist
      DeltaSchemaMigration.createOrUpdateSchema(
        logTableName,
        jobConf.deltalake.tablePath,
        df.schema,
        jobConf.deltalake.schemaEvolution,
        spark
      )

      val readQuery = df.writeStream
        .option(jobConf.deltalake.schemaEvolution)
        .option("checkpointLocation", s"${jobConf.checkpointDir}/$logTableName")
        .format("delta")
        .outputMode("append")
        .queryName(s"${logTableName}_write")

      jobConf.deltalake
        .applyTrigger(readQuery)
        .start(logTablePath)

      val logTableStreamingDf = spark.readStream
        .format("delta")
        .load(logTablePath)

      jobConf.deltalake
        .applyPartitioning(logTableStreamingDf)
    }

    logger.info(
      s"defining stream for Datastream table source located at $paths")

    val streamingInputDf = spark.readStream
      .format(jobConf.datastream.readFormat.value)
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger",
              jobConf.datastream.fileReadConcurrency.value)
      .load(filePaths(datastreamTable.tablePath))

    val repartitioned = jobConf.deltalake.applyPartitioning(streamingInputDf)

    if (jobConf.generateLogTable) {
      logTableStreamFrom(repartitioned)
    } else {
      repartitioned
    }
  }

  private def filePaths(path: String): String =
    s"$path/*/*/*/*/*"
}
