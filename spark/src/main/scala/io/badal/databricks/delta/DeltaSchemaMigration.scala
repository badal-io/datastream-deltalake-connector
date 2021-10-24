package io.badal.databricks.delta

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.config.SchemaEvolutionStrategy._
import io.badal.databricks.utils.TableMetadata
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.Try

object DeltaSchemaMigration {

  /** A struct field that is added to the target table to maintain important Datastream metadata */
  val DatastreamMetadataField = "datastream_metadata"

  private val log = Logger.getLogger(getClass.getName)

//  def createTableIfDoesNotExist(tableName: String, schema: StructType)(
//      implicit spark: SparkSession) = {
//    val exists = doesTableExist(tableName)
//    if (!exists) {
//      val emptyDF =
//        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
//
//      log.info(s"Creating table $tableName")
//
//      emptyDF.write
//        .format("delta")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable(tableName)
//    }
//  }

  /** Update Table schema.
    * Simplest way to do this is to append and empty dataframe to the table with mergeSchema=true
    * */
  def updateSchemaByName(tableName: String,
                         tableMetadata: TableMetadata,
                         schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DeltaTable = {

    // TODO: There may be a cleaner way to do this - instead of always appending an empty Dataframe,
    // may want to first check if schema has changed. Though it is quite possible that DeltaLake
    // takes care of these optimizations under the hood see the commented out migrateTableSchema
    // function below for another way of doing this
    val schema = buildTargetSchema(tableMetadata.payloadSchema,
                                   tableMetadata.orderByFieldsSchema)

    updateSchemaByName(tableName, schema, schemaEvolutionStrategy)
  }

  def updateSchemaByName(tableName: String,
                         schema: StructType,
                         schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DeltaTable = {
    val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    log.info(s"Target schema for table $tableName is  $schema")

    emptyDF.write
      .option(schemaEvolutionStrategy)
      .format("delta")
      .mode(SaveMode.Append)
      .saveAsTable(tableName)

    DeltaTable.forName(tableName)
  }

  def updateSchemaByPath(path: String,
                         schema: StructType,
                         schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DeltaTable = {
    val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    log.info(s"Target schema for path $path is  $schema")

    emptyDF.write
      .option(schemaEvolutionStrategy)
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    DeltaTable.forPath(path)
  }

  def buildTargetSchema(payloadSchema: StructType,
                        datastreamMetadataSchema: StructType): StructType =
    payloadSchema.add(DatastreamMetadataField, datastreamMetadataSchema)

  private def doesTableExist(tableName: String): Boolean =
    Try(DeltaTable.forName("target")).isSuccess

}

trait DeltaSchemaMigration {}
