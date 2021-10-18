package io.badal.databricks.datastream

final case class DatastreamTable(bucket: String,
                                 bucketPath: String,
                                 database: String,
                                 table: String) {
  val path = s"$bucket/$bucketPath/$database.$table"
}
