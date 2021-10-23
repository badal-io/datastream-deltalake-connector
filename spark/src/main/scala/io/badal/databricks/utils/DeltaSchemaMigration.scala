package io.badal.databricks.utils

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.config.SchemaEvolutionStrategy._
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.util.Try

object DeltaSchemaMigration {

  /** A struct field that is added to the target table to maintain important Datastream metadata */
  val DatastreamMetadataField = "datastream_metadata"

  val log = Logger.getLogger(getClass.getName)

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
    //TODO There may be a cleaner way to do this - instead of always appending an empty Dataframe, may want to first check if schema has changed
    // Though it is quite possible that DeltaLake takes care of these optimizations under the hood
    // see the commented out migrateTableSchema function bellow for another way of doing this
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
                        datastreamMetadataSchema: StructType) =
    payloadSchema.add(DatastreamMetadataField, datastreamMetadataSchema)

  private def doesTableExist(tableName: String): Boolean =
    Try(DeltaTable.forName("target")).isSuccess

}

trait DeltaSchemaMigration {
  //  def getPayloadSchema(df: DataFrame): StructType
  //  def getPayloadFields(df: DataFrame): Seq[String]
  //
  //  def run(tablePath: String, df: DataFrame)(implicit spark: SparkSession) = {
  //    createTableIfDoesNotExist(tablePath, df.schema)
  //
  //    val table = DeltaTable.forName(tablePath)
  //
  //    // check if schema migration is required
  //    if (!SchemaUtils.isReadCompatible(table.toDF.schema, df.schema)) {
  //      val newSchema = SchemaUtils.mergeSchemas(table.toDF.schema, df.schema)
  //
  //      newSchema
  //    }
  //
  //
  //
  //  }

  //  /**
  //   * Migrate schema of target table to match schema of the source table
  //   * @param tablePath
  //   * @param df
  //   * @param ignoreFields
  //   * DeltaLake supports only adding new fields.
  //   * The only reason this method is needed is because auto-schema migration is supported for merge
  //   *    operations only when updateAll/insertAll is used.
  //   */
  //  private def migrateTableSchema(tablePath: String, df: DataFrame, ignoreFields: util.Set[String]): Unit = {
  //    //val tableLock: TableId = getTableLock(tableId)
  //   // tableLock synchronized
  //    //val table: Table = this.tableCache.get(tableId)
  //    val targetTableSchema: StructType = DeltaTable.forPath(tablePath).toDF.schema
  //    val inputSchema: StructType = getPayloadSchema(df)
  //
  //    targetTableSchema.f
  //    val newFieldList: util.List[Field] = getNewTableFields(row, table, inputSchema, ignoreFields)
  //    if (newFieldList.size > 0) { // Add all current columns to the list
  //      val fieldList: util.List[Field] = new util.ArrayList[Field]
  //      import scala.collection.JavaConversions._
  //      for (field <- table.getDefinition.getSchema.getFields) {
  //        fieldList.add(field)
  //      }
  //      // Add all new columns to the list
  //      LOG.info("Mapping New Columns for: {} -> {}", tableId.toString, newFieldList.toString)
  //      import scala.collection.JavaConversions._
  //      for (field <- newFieldList) {
  //        fieldList.add(field)
  //      }
  //      val newSchema: Schema = Schema.of(fieldList)
  //      val updatedTable: Table = table.toBuilder.setDefinition(StandardTableDefinition.of(newSchema)).build.update
  //      LOG.info("Updated Table: {}", tableId.toString)
  //      this.tableCache.reset(tableId, table)
  //    }
  //
  //  }

}
