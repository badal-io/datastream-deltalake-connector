package io.badal.databricks.delta

import io.badal.databricks.config.{DatastreamDeltaConf, SchemaEvolutionStrategy}
import io.badal.databricks.datastream.DatastreamTable
import io.badal.databricks.delta.DeltaSchemaMigration.buildTargetSchema
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

object DeltaSchemaMigration {

  /** A struct field that is added to the target table to maintain important Datastream metadata */
  val DatastreamMetadataField = "datastream_metadata"

  private val log = Logger.getLogger(getClass.getName)

  def createAll(datastreamTables: Seq[DatastreamTable],
                jobConf: DatastreamDeltaConf,
                spark: SparkSession): Unit = {

    datastreamTables.foreach { datastreamTable =>
      val df = spark.read
        .format(jobConf.datastream.readFormat.value)
        .option("ignoreExtension", true)
        .option("maxFilesPerTrigger",
                jobConf.datastream.fileReadConcurrency.value)
        .load(datastreamTable.tablePath + "/*/*/*/*/*")
        .limit(1)

      create(datastreamTable,
             df,
             jobConf.deltalake.tablePath.value,
             jobConf.generateLogTable)
    }
  }

  def create(datastreamTable: DatastreamTable,
             df: DataFrame,
             path: String,
             generateLogTable: Boolean): Unit = {
    val mergeTableName =
      TableNameFormatter.targetTableName(datastreamTable.table)
    val mergeTablePath = s"$path/$mergeTableName"

    val mergeTableSchema = buildTargetSchema(TableMetadata.fromDf(df))

    log.info(
      s"Creating merge table $mergeTableName " +
        s"with schema $mergeTableSchema if it is not present")

    createIfNotExists(mergeTableSchema, mergeTableName, mergeTablePath)

    if (generateLogTable) {
      val logTableName =
        TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"$path/$logTableName"
      createIfNotExists(df.schema, logTableName, logTablePath)
    }
  }

  /** Update Table schema.
    * Simplest way to do this is to append and empty dataframe to the table with mergeSchema=true
    * */
  def updateSchema(path: String,
                   tableMetadata: TableMetadata,
                   schemaEvolutionStrategy: SchemaEvolutionStrategy)(
      implicit spark: SparkSession): DeltaTable = {

    // TODO: There may be a cleaner way to do this - instead of always appending an empty Dataframe,
    // may want to first check if schema has changed. Though it is quite possible that DeltaLake
    // takes care of these optimizations under the hood see the commented out migrateTableSchema
    // function below for another way of doing this
    val schema = buildTargetSchema(tableMetadata)
    val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    log.info(s"Target schema for table at path $path is  $schema")

    emptyDF.write
      .option(schemaEvolutionStrategy)
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    DeltaTable.forPath(path)
  }

  private def createIfNotExists(schema: StructType,
                                name: String,
                                path: String) = {
    log.info(s"Creating delta table $name at path $path if it is not present")

    val baseCmd = DeltaTable
      .createIfNotExists()
      .tableName(name)
      .location(path)

    schema.fields
      .foldLeft(baseCmd) { (acc, field) =>
        acc.addColumn(field)
      }
      .execute()
  }

  /** Append Metadata fields */
  def buildTargetSchema(tableMetadata: TableMetadata): StructType =
    tableMetadata.orderByFields.foldLeft(tableMetadata.payloadSchema) {
      case (schema, (field, fieldType)) =>
        schema.add(
          StructField(datastreamMetadataTargetFieldName(field),
                      fieldType,
                      nullable = false))
    }

  /** Flatten out and rename datastream metadata fields when writing to target */
  def datastreamMetadataTargetFieldName(field: String): String =
    s"${DatastreamMetadataField}_${field.replace(".", "_")}"
}
