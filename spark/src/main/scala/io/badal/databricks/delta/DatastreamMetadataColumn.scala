package io.badal.databricks.delta

import io.badal.databricks.delta.DatastreamMetadataColumn.DatastreamMetadataField
import org.apache.spark.sql.types.{DataType, StructField}

case class DatastreamMetadataColumn(fieldName: String, `type`: DataType) {

  /** Delta doesn't support nested columns in MERGE statements.
    * Flatten out and rename Datastream metadata fields when writing to target */
  def toTargetFieldName: String =
    s"${DatastreamMetadataField}_${fieldName.replace(".", "_")}"
  def getTargetFieldSchema: StructField =
    StructField(toTargetFieldName, `type`, nullable = false)
}
object DatastreamMetadataColumn {

  /** A struct field that is added to the target table to maintain important Datastream metadata */
  private val DatastreamMetadataField = "datastream_metadata"
}
