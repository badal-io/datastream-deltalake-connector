package io.badal.databricks.datastream

final case class DatastreamTable(path: String, table: String) {
  val tablePath = s"$path/$table"
}
