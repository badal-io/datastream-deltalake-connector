package io.badal.databricks.jobs

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.datastream.DatastreamIO
import io.badal.databricks.delta.{
  DeltaSchemaMigration,
  MergeQueries,
  TableNameFormatter
}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatastreamDeltaConnector {

  val logger = Logger.getLogger(DatastreamDeltaConnector.getClass)

  def run(spark: SparkSession, jobConf: DatastreamDeltaConf): Unit = {
    logger.info("starting...")

    val tables = jobConf.datastream.tableSource.list()

    DeltaSchemaMigration.createAll(tables, jobConf, spark)

    tables.foreach { datastreamTable =>
      logger.info(
        s"defining stream for datastream table defined at ${datastreamTable.tablePath}")

      /** Get a streaming Dataframe of Datastream records */
      val inputDf = DatastreamIO.readStreamFor(datastreamTable, jobConf, spark)

      val targetTable =
        TableNameFormatter.targetTableName(datastreamTable.table)

      /** Merge into target table */
      inputDf.writeStream
        .format("delta")
        .option(jobConf.deltalake.schemaEvolution)
        .option("checkpointLocation", s"${jobConf.checkpointDir}/$targetTable")
        .foreachBatch { (df: DataFrame, batchId: Long) =>
          MergeQueries.upsertToDelta(
            df,
            batchId,
            jobConf.deltalake.schemaEvolution,
            jobConf.deltalake.tablePath.value
          )
        }
        .outputMode("update")
        .start()
    }

    spark.streams.awaitAnyTermination()
  }

}
