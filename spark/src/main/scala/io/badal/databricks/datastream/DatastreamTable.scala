package io.badal.databricks.datastream

final case class DatastreamTable(bucket: String,
                                 bucketPath: String,
                                 table: String) {
  val path = s"$bucket/$bucketPath$table"
  val rawTableName = s"delta/$table-cdc"
}
