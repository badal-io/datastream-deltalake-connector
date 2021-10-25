package io.badal.databricks.jobs

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.datastream.DatastreamIO
import io.badal.databricks.delta.MergeQueries
import io.badal.databricks.utils.TableNameFormatter
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatastreamDeltaConnector {

  val logger = Logger.getLogger(DatastreamDeltaConnector.getClass)

  def run(spark: SparkSession, jobConf: DatastreamDeltaConf): Unit = {
    logger.info("starting...")
    // TODO: Remove - get Database from TableMetadata
    val tables = jobConf.datastream.tableSource.list()

    tables.foreach { datastreamTable =>
      logger.info(
        s"defining stream for datastream table defined at ${datastreamTable.tablePath}")

      /** Get a streaming Dataframe of Datastream records */
      val inputDf = DatastreamIO(
        spark,
        datastreamTable,
        jobConf.datastream.fileReadConcurrency.value,
        jobConf.generateLogTable,
        jobConf.checkpointDir,
        jobConf.deltalake.schemaEvolution,
        jobConf.datastream.readFormat.value
      )

      val targetTable =
        TableNameFormatter.targetTableName(datastreamTable.table)

      /** Merge into target table */
      inputDf.writeStream
        .format("delta")
        .option(jobConf.deltalake.schemaEvolution)
        .option("checkpointLocation",
          s"${jobConf.checkpointDir}/$targetTable")
        .foreachBatch((df: DataFrame, batchId: Long) =>
          MergeQueries
            .upsertToDelta(df, batchId, jobConf.deltalake.schemaEvolution))
        .outputMode("update")
        .start(s"/delta/$targetTable")
    }

    spark.streams.awaitAnyTermination()

  }


}
