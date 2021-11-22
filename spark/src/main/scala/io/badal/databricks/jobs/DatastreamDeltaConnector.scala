package io.badal.databricks.jobs

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.datastream.DatastreamIO
import io.badal.databricks.delta.MergeQueries.log
import io.badal.databricks.delta.{
  DatastreamDeltaTable,
  DeltaSchemaMigration,
  MergeQueries,
  TableMetadata
}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success}

object DatastreamDeltaConnector {

  val logger = Logger.getLogger(DatastreamDeltaConnector.getClass)

  def run(spark: SparkSession, jobConf: DatastreamDeltaConf): Unit = {
    logger.info("starting...")

    jobConf.deltalake.compaction.foreach(_.applyTo(spark))
    jobConf.deltalake.optimize.foreach(_.applyTo(spark))

    logger.info(
      s"loading table targets using: ${jobConf.datastream.tableSource}")

    val tables = jobConf.datastream.tableSource.list()

    logger.info("table targets to be loaded into delta...")
    tables.foreach(table => logger.info(s"table: ${table.table}"))

    tables.foreach { datastreamTable =>
      logger.info(
        s"defining stream for datastream table defined at ${datastreamTable.tablePath}")

      DatastreamIO.readTableMetadata(datastreamTable, jobConf, spark) match {

        /** Override the inferred db name if its been explicitly defined in the configuration */
        case Success(inferredTableMeta: TableMetadata) =>
          val tableMetadata = jobConf.deltalake.database match {
            case Some(dbOverride) =>
              val withDbOverride: DatastreamDeltaTable = inferredTableMeta.table
                .copy(databaseName = dbOverride.value)

              inferredTableMeta.copy(table = withDbOverride)
            case None =>
              /** Make sure Database exists */
              DeltaSchemaMigration.createDBIfNotExist(
                inferredTableMeta.table,
                jobConf.deltalake.tablePath.value
              )(spark)

              inferredTableMeta
          }

          /** Get a streaming Dataframe of Datastream records */
          val inputDf = DatastreamIO.readStreamFor(
            datastreamTable,
            tableMetadata,
            jobConf,
            spark
          )

          /** Merge into target table */
          val mergeQuery: DataStreamWriter[Row] = inputDf.writeStream
            .format("delta")
            .option(jobConf.deltalake.schemaEvolution)
            .option(
              "checkpointLocation",
              s"${jobConf.checkpointDir}/${tableMetadata.table.fullTargetTableName}")
            .foreachBatch { (df: DataFrame, _: Long) =>
              MergeQueries.upsertToDelta(
                df,
                jobConf.deltalake.schemaEvolution,
                jobConf.deltalake.tablePath
              )
            }
            .outputMode("update")
            .queryName(s"${tableMetadata.table.fullTargetTableName}_write")

          jobConf.deltalake
            .applyTrigger(mergeQuery)
            .start()

        case Failure(_) =>
          log.error(
            s"empty folder ${datastreamTable.tablePath} " +
              s" for table $datastreamTable - could not start")
      }
    }
  }
}
