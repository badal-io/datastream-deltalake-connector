package io.badal.databricks

import io.badal.databricks.config.Config.DatastreamJobConf
import io.badal.databricks.utils.{
  DataStreamSchema,
  DatastreamIO,
  MergeQueries,
  MergeSettings
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource

/* Don't remove, Intellij thinks they are unused but they are required */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._

object DatastreamDatabricksConnector {
  def main(args: Array[String]): Unit = {
    val jobConf: DatastreamJobConf =
      ConfigSource.resources("demo.conf").loadOrThrow[DatastreamJobConf]

    // TODO
    Logger.getRootLogger.setLevel(Level.ERROR)

    /** Create a spark session */
    val spark = SparkSession.builder
      .appName("DatastreamReader")
      //.config("spark.master", "local")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    DataStreamSchema.registerIfNotExists(spark,
                                         jobConf.datastream.database.value)

    // todo: support multi table
    val table = jobConf.tables.headOption match {
      case Some(tableConf) => tableConf
      case None =>
        throw new IllegalArgumentException(
          "At least one datastream table should be provided")
    }

    val bucket = s"${jobConf.path(table.name.value)}/*/*/*/*/*"

    /** Get a streaming Dataframe of Datastream records*/
    val inputDf =
      DatastreamIO(spark, bucket, jobConf.datastream.fileReadConcurrency.value)

    val mergeSettings: MergeQueries = MergeQueries(
      MergeSettings(
        targetTableName = table.name.value,
        idColName = table.primaryKey.value,
        tsColName = table.timestamp.value,
        spark = spark
      ))

    /** Merge into target table*/
    val query = inputDf.writeStream
      .format("delta")
      .foreachBatch(mergeSettings.upsertToDelta _)
      .outputMode("update")
      //   .option("checkpointLocation", "dbfs:/checkpointPath")
      .start()

    query.awaitTermination()
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
