package io.badal.databricks

import io.badal.databricks.config.DatastreamJobConf
import io.badal.databricks.utils.{
  DatastreamIO,
  MergeQueries,
  TableNameFormatter
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.ConfigSource

/* Don't remove, Intellij thinks they are unused but they are required */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

object DatastreamDatabricksConnector {

  val logger = Logger.getLogger(DatastreamDatabricksConnector.getClass)

  def main(args: Array[String]): Unit = {

    val jobConf: DatastreamJobConf =
      ConfigSource.resources("demo.conf").loadOrThrow[DatastreamJobConf]

    // TODO
    Logger.getRootLogger.setLevel(Level.ERROR)

    /** Create a spark session */
    implicit val spark = SparkSession.builder
      .appName("DatastreamReader")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    // TODO: Remove - get Database from TableMetadata
    val tables = jobConf.datastream.tableSource.list()

    tables.foreach { datastreamTable =>
      logger.info(
        s"defining stream for datastream table defined at ${datastreamTable.tablePath}")

      /** Get a streaming Dataframe of Datastream records */
      val inputDf = DatastreamIO(
        datastreamTable,
        jobConf.datastream.fileReadConcurrency.value,
        jobConf.generateLogTable,
        jobConf.checkpointDir,
        jobConf.deltalake.schemaEvolution,
        jobConf.datastream.readFormat
      )

      val targetTable =
        TableNameFormatter.targetTableName(datastreamTable.table)

      /** Merge into target table */
      inputDf.writeStream
        .format("delta")
        .option(jobConf.deltalake.schemaEvolution)
        .option("checkpointLocation",
                s"dbfs:/${jobConf.checkpointDir}/$targetTable")
        .foreachBatch((df: DataFrame, batchId: Long) =>
          MergeQueries
            .upsertToDelta(df, batchId, jobConf.deltalake.schemaEvolution))
        .outputMode("update")
        .start(s"/delta/$targetTable")
    }

    spark.streams.awaitAnyTermination()
  }

}
