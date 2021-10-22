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
        s"defining stream for datastream table defined at ${datastreamTable.path}")

      /** Get a streaming Dataframe of Datastream records */
      val inputDf = DatastreamIO(
        datastreamTable,
        jobConf.datastream.fileReadConcurrency.value,
        jobConf.generateLogTable,
        jobConf.checkpointDir,
        jobConf.deltalake.schemaEvolution
      )

      val targetTable =
        TableNameFormatter.targetTableName(datastreamTable.table)

      /** Merge into target table */
      inputDf.writeStream
        .format("delta")
        .option(jobConf.deltalake.schemaEvolution)
//        .option("checkpointLocation", s"dbfs:/${jobConf.checkpointDir}/$targetTable")
        .foreachBatch((df: DataFrame, batchId: Long) =>
          MergeQueries
            .upsertToDelta(df, batchId, jobConf.deltalake.schemaEvolution))
        .outputMode("update")
        .start(s"/delta/$targetTable")
    }

    spark.streams.awaitAnyTermination()
    //    val latestChangeForEachKey = MergeQueries.getLatestRow(inputDf, "id", "source_timestamp")
    //
    //    val query = inputDf.writeStream
    //      .format("delta")
    //      .outputMode("append")
    //      .option("checkpointLocation", "dbfs://checkpointPath")
    //      .option("mergeSchema", "true")
    //      //.toTable("")
    //      .delta("/mnt/delta/events")
    //    DeltaTable.forName("target")
    //
    //    val targetTable = DeltaTable.forPath(spark, "/data/votes2/")
    //
    //    voterTable.alias("t").merge(latestChangesDF.alias("s"), "t.id =s.payload.id").whenMatchedDelete(condition = "s.source_metadata.change_type = 'DELETE'") \
    //    .whenMatchedUpdate(set = COLUMNS_MAP) \
    //    .whenNotMatchedInsert(condition = "s.source_metadata.change_type != 'DELETE'", values = COLUMNS_MAP
    //    )

    /**
      * "USING ({stagingViewSql}) AS {stagingAlias} ",
      * "ON {joinCondition} ",
      * "WHEN MATCHED AND {timestampCompareSql} AND {stagingAlias}.{deleteColumn}=True THEN DELETE ",// TODO entire block should be configurably removed
      * "WHEN MATCHED AND {timestampCompareSql} THEN {mergeUpdateSql} ",
      * "WHEN NOT MATCHED BY TARGET AND {stagingAlias}.{deleteColumn}!=True ",
      * "THEN {mergeInsertSql}")
      */
    //    targetTable.as("t")
    //      .merge(
    //        latestChangeForEachKey.as("s"), "t.id = s.payload.id"
    //      )
    //      .whenMatched().updateAll()
    //      .whenMatched()
    //      .updateExpr(Map("key" -> "s.key", "value" -> "s.newValue"))
    //      .whenNotMatched("s.deleted = false")
    //      .insertExpr(Map("key" -> "s.key", "value" -> "s.newValue"))
    //      .execute()

    //  query.awaitTermination()

  }

}
