package io.badal.databricks.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.util.Try

object DeltaSchemaMigration {
  def createTableIfDoesNotExist(tableName: String, schema: StructType)(
      implicit spark: SparkSession) = {
    val exists = doesTableExist(tableName)
    if (!exists) {
      val emptyDF =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      emptyDF.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  /** Update Table schema.
    * Simplest way to do this is to append and empty dataframe to the table with mergeSchema=true
    * */
  def updateSchema(tableName: String, sourceSchema: StructType)(
      implicit spark: SparkSession): DeltaTable = {
   //TODO
    /* val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sourceSchema)
    emptyDF.write
      .option("mergeSchema", "true")
      .format("delta")
      .mode(SaveMode.Append)
      .saveAsTable(tableName)
*/
    DeltaTable.forName(tableName)

  }

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
