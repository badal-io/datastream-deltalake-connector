package io.badal.databricks.config

import eu.timepit.refined.types.string.NonEmptyString
import eu.timepit.refined.types.numeric.{PosInt, PosLong}
import io.badal.databricks.datastream.TableProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

import scala.concurrent.duration.FiniteDuration

final case class DatastreamDeltaConf(
    datastream: DatastreamConf,
    deltalake: DeltalakeConf,
    generateLogTable: Boolean,
    checkpointDir: String
)

final case class DatastreamConf(
    name: NonEmptyString,
    startDate: Option[NonEmptyString],
    fileReadConcurrency: PosInt,
    tableSource: TableProvider,
    readFormat: NonEmptyString
)

final case class DeltalakeConf(
    tableNamePrefix: String,
    mergeFrequency: Option[FiniteDuration],
    schemaEvolution: SchemaEvolutionStrategy,
    tablePath: NonEmptyString,
    compaction: Option[DeltalakeCompactionConf],
    optimize: Option[DeltalakeOptimizeConf],
    database: Option[NonEmptyString]
) {

  def applyTrigger(writer: DataStreamWriter[Row]): DataStreamWriter[Row] =
    mergeFrequency match {
      case Some(interval) =>
        writer.trigger(Trigger.ProcessingTime(interval))
      case None =>
        writer
    }
}

final case class DeltalakeCompactionConf(
    autoCompactionEnabled: Boolean,
    minNumberOfFiles: Option[PosInt],
    maxFileSizeBytes: Option[PosLong],
    targetFileSizeBytes: Option[PosLong]
) {
  import Config._

  def applyTo(spark: SparkSession): Unit = {
    logger.info(s"setting $COMPACTION_AUTO_ENABLED $autoCompactionEnabled")
    spark.conf.set(COMPACTION_AUTO_ENABLED, autoCompactionEnabled)

    minNumberOfFiles.foreach { minNumberOfFilesSafe =>
      logger.info(
        s"setting $COMPACTION_MIN_NUM_FILES = ${minNumberOfFilesSafe.value}")
      spark.conf.set(COMPACTION_MIN_NUM_FILES, minNumberOfFilesSafe.value)
    }

    maxFileSizeBytes.foreach { maxFileSizeBytesSafe =>
      logger.info(
        s"setting $COMPACTION_MAX_FILE_SIZE = ${maxFileSizeBytesSafe.value}")
      spark.conf.set(COMPACTION_MAX_FILE_SIZE, maxFileSizeBytesSafe.value)
    }

    targetFileSizeBytes.foreach { targetFileSizeBytesSafe =>
      logger.info(
        s"setting $TARGET_FILE_SIZE = ${targetFileSizeBytesSafe.value}")
      spark.conf.set(TARGET_FILE_SIZE, targetFileSizeBytesSafe.value)
    }
  }
}

final case class DeltalakeOptimizeConf(
    autoOptimizeEnabled: Boolean,
    batchInterval: Option[PosInt],
    maxFileSizeBytes: Option[PosLong]
) {
  import Config._

  def applyTo(spark: SparkSession): Unit = {
    logger.info(s"setting $AUTO_OPTIMIZE_ENABLED $autoOptimizeEnabled")
    spark.conf.set(AUTO_OPTIMIZE_ENABLED, autoOptimizeEnabled)
  }
}

object Config {

  val logger = Logger.getLogger(Config.getClass)

  val AUTO_OPTIMIZE_ENABLED =
    "spark.databricks.delta.optimizeWrite.enabled"
  val COMPACTION_AUTO_ENABLED = "spark.databricks.delta.autoCompact.enabled"
  val COMPACTION_MIN_NUM_FILES =
    "spark.databricks.delta.autoCompact.minNumFiles"
  val COMPACTION_MAX_FILE_SIZE =
    "spark.databricks.delta.autoCompact.maxFileSize"
  val TARGET_FILE_SIZE =
    "spark.databricks.delta.targetFileSize"
}
