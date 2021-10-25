package io.badal.databricks.delta

import io.badal.databricks.datastream.{DataStreamSchema, DatastreamSource, MySQL, Oracle}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * Description of a Datastream tables including all context required to execute a Delta merge
  */
case class TableMetadata(sourceType: DatastreamSource,
                         table: String,
                         database: String,
                         payloadPrimaryKeyFields: Seq[String],
                         orderByFieldsSchema: StructType,
                         payloadSchema: StructType) {
  lazy val orderByFields: Seq[String] = orderByFieldsSchema.fieldNames
}

object TableMetadata {
  private val ORACLE_ORDER_BY_FIELDS =
    Seq("source_timestamp", "source_metadata.scn")
  private val MYSQL_ORDER_BY_FIELDS = Seq("source_timestamp",
                                          "source_metadata.log_file",
                                          "source_metadata.log_position")
  private val ORACLE_ORDER_BY_FIELDS_SCHEMA = new StructType(
    Array(
      StructField("source_timestamp", TimestampType, true),
      StructField("source_metadata.scn", LongType, true)
    ))
  private val MYSQL_ORDER_BY_FIELDS_SCHEMA = new StructType(
    Array(
      StructField("source_timestamp", TimestampType, true),
      StructField("source_metadata.log_file", StringType, true),
      StructField("source_metadata.log_position", LongType, true)
    ))
  private val METADATA_DELETED = "_metadata_deleted"

  /**
    * Return a TableMetadata by inspecting the first elements of the given DataFrame
    */
  def fromDf(df: DataFrame): TableMetadata = {
    import org.apache.spark.sql.functions._

    val payloadSchema = DataStreamSchema.payloadSchema(df)

    val head = df
      .select(
        col("read_method").as("read_method"),
        col("source_metadata.table").as("table"),
        col("source_metadata.database").as("database"),
        col("source_metadata.primary_keys").as("primary_keys")
      )
      .head

    val source = getSourceTypeFromReadMethod(head.getAs[String]("read_method"))

    TableMetadata(
      sourceType = source,
      table = head.getAs("table"),
      database = head.getAs("database"),
      payloadPrimaryKeyFields = head.getAs("primary_keys"),
      orderByFieldsSchema = getOrderByFieldsSchema(source),
      payloadSchema = payloadSchema
    )
  }

  private def getSourceTypeFromReadMethod(
      readMethod: String): DatastreamSource =
    readMethod.split("-")(0) match {
      case "mysql"  => MySQL
      case "oracle" => Oracle
    }

  private def getOrderByFieldsSchema(source: DatastreamSource): StructType =
    source match {
      case MySQL  => MYSQL_ORDER_BY_FIELDS_SCHEMA
      case Oracle => ORACLE_ORDER_BY_FIELDS_SCHEMA
    }

}
