package io.badal.databricks.delta

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.SchemaEvolutionStrategy
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object DeltaSchemaMigration {

  /** A struct field that is added to the target table to maintain important Datastream metadata */
  val DatastreamMetadataField = "datastream_metadata"

  private val log = Logger.getLogger(getClass.getName)

  def createDBIfNotExist(table: DatastreamDeltaTable,
                         path: String,
                         comment: String = "Replicated from Datastream")(
      implicit spark: SparkSession): DataFrame =
    spark.sql(s"""CREATE DATABASE IF NOT EXISTS ${table.databaseName}
         | COMMENT '$comment' LOCATION '${path}'""".stripMargin)

  /** Update Table schema.
    * Simplest way to do this is to append and empty dataframe to the table with mergeSchema=true
    * */
  def createOrUpdateSchema(basePath: NonEmptyString,
                           tableMetadata: TableMetadata,
                           schemaEvolutionStrategy: SchemaEvolutionStrategy,
                           spark: SparkSession): DeltaTable = {

    // TODO: There may be a cleaner way to do this - instead of always appending an empty Dataframe,
    // may want to first check if schema has changed. Though it is quite possible that DeltaLake
    // takes care of these optimizations under the hood see the commented out migrateTableSchema
    // function below for another way of doing this
    val schema = buildTargetSchema(tableMetadata)

    createOrUpdateSchema(tableMetadata.table,
                         basePath,
                         schema,
                         schemaEvolutionStrategy,
                         spark)
  }

  private def createOrUpdateSchema(
      table: DatastreamDeltaTable,
      path: NonEmptyString,
      schema: StructType,
      schemaEvolutionStrategy: SchemaEvolutionStrategy,
      spark: SparkSession): DeltaTable = {
    val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    log.info(s"Target schema for table at path $path is  $schema")

    emptyDF.write
      .option(schemaEvolutionStrategy)
      .option("path", path.value)
      .format("delta")
      .mode(SaveMode.Append)
      .saveAsTable(table.fullTargetTableName)

    DeltaTable.forName(table.fullTargetTableName)
  }

  /** Append Metadata fields */
  def buildTargetSchema(tableMetadata: TableMetadata): StructType =
    tableMetadata.orderByFields.foldLeft(tableMetadata.payloadSchema) {
      case (schema, field) => schema.add(field.getTargetFieldSchema)
    }

  /** Flatten out and rename datastream metadata fields when writing to target */
  def datastreamMetadataTargetFieldName(field: String): String =
    s"${DatastreamMetadataField}_${field.replace(".", "_")}"
}
