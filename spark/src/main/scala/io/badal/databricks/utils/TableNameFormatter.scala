package io.badal.databricks.utils

object TableNameFormatter {
  def targetTableName(tableName: String) = sanitizedTable(tableName)
  def logTableName(tableName: String) = s"${sanitizedTable(tableName)}_log"
  def sanitizedTable(tableName: String) = tableName.replace('.', '_')
}
