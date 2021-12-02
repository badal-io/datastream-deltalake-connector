package io.badal.databricks.delta

import io.badal.databricks.datastream.{
  DataStreamSchema,
  DatastreamSource,
  MySQL,
  Oracle
}
import io.badal.databricks.delta
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** Describes everything we need to know about a Table to make a proper Merge Query */
case class TableMetadata(sourceType: DatastreamSource,
                         table: DatastreamDeltaTable,
                         payloadPrimaryKeyFields: Seq[String],
                         /** primary keys - are part of the stream message 'payload' object */
                         orderByFields: Seq[DatastreamMetadataColumn],
                         /** ordered fields that can be used to order messages */
                         payloadSchema: StructType, /** Payload schema */
                         payloadFields: Seq[String]) {}

object TableMetadata {
  private[delta] val ORACLE_ORDER_BY_FIELDS =
    Seq(DatastreamMetadataColumn("source_timestamp", TimestampType),
        DatastreamMetadataColumn("source_metadata.scn", LongType))
  private[delta] val MYSQL_ORDER_BY_FIELDS =
    Seq(
      DatastreamMetadataColumn("source_timestamp", TimestampType),
      DatastreamMetadataColumn("source_metadata.log_file", StringType),
      DatastreamMetadataColumn("source_metadata.log_position", LongType)
    )

  def fromDfUnsafe(df: DataFrame): TableMetadata = fromDf(df).get

  /** Gets the TableMetadata by inspecting the first elements of a Dataframe */
  def fromDf(df: DataFrame,
             dbOverride: Option[String] = None): Option[TableMetadata] = {
    import org.apache.spark.sql.functions._

    df.select(
        col("read_method").as("read_method"),
        col("source_metadata.table").as("table"),
        col("source_metadata.database").as("database"),
        col("source_metadata.primary_keys").as("primary_keys")
      )
      .head(1)
      .headOption
      .map { head =>
        val payloadSchema: StructType = DataStreamSchema.payloadSchema(df)
        val payloadFields: Array[String] = DataStreamSchema.payloadFields(df)
        val source =
          getSourceTypeFromReadMethod(head.getAs[String]("read_method"))

        val table = head.getAs[String]("table")
        val database = dbOverride.getOrElse(head.getAs[String]("database"))

        delta.TableMetadata(
          sourceType = source,
          table =
            DatastreamDeltaTable(databaseName = database, tableName = table),
          payloadPrimaryKeyFields = head.getAs("primary_keys"),
          orderByFields = getOrderByFields(source),
          payloadSchema = payloadSchema,
          payloadFields = payloadFields
        )
      }
  }

  private def getSourceTypeFromReadMethod(
      readMethod: String): DatastreamSource =
    readMethod.split("-")(0) match {
      case "mysql"  => MySQL
      case "oracle" => Oracle
    }

  private def getOrderByFields(
      source: DatastreamSource): Seq[DatastreamMetadataColumn] =
    source match {
      case MySQL  => MYSQL_ORDER_BY_FIELDS
      case Oracle => ORACLE_ORDER_BY_FIELDS
    }

}
