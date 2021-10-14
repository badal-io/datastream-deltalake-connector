package io.badal.databricks.datastream

final case class DatastreamTable(bucket: String,
                                 database: String,
                                 table: String) {
  val path = s"$bucket/$database.$table"
}
