package io.badal.databricks.delta

case class DatastreamDeltaTable(databaseName: String, tableName: String) {
  def fullTargetTableName: String =
    s"$databaseName.${sanitizedTable(tableName)}"
  def fullLogTableName: String =
    s"$databaseName.${sanitizedTable(tableName)}_log"

  private def sanitizedTable(tableName: String): String =
    tableName.replace('.', '_')
}
