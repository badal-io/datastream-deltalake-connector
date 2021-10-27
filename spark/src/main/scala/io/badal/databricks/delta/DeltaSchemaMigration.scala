package io.badal.databricks.delta

import io.badal.databricks.config.DatastreamDeltaConf
import io.badal.databricks.datastream.DatastreamTable
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

object DeltaSchemaMigration {

  case class DeltaTarget(logTableOpt: Option[DeltaTable],
                         mergeTable: DeltaTable)

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

      create(datastreamTable, df, jobConf)
    }
  }

  def create(datastreamTable: DatastreamTable,
             df: DataFrame,
             jobConf: DatastreamDeltaConf): Unit = {
    val mergeTableName =
      TableNameFormatter.targetTableName(datastreamTable.table)
    val mergeTablePath = s"${jobConf.deltalake.tablePath}/$mergeTableName"

    val mergeTableSchema = buildTargetSchema(TableMetadata.fromDf(df))

    log.info(
      s"Creating merge table $mergeTableName " +
        s"with schema $mergeTableSchema if it is not present")

    createIfNotExists(mergeTableSchema, mergeTableName, mergeTablePath)

    if (jobConf.generateLogTable) {
      val logTableName =
        TableNameFormatter.logTableName(datastreamTable.table)
      val logTablePath = s"${jobConf.deltalake.tablePath}/$logTableName"
      createIfNotExists(df.schema, logTableName, logTablePath)
    }
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
