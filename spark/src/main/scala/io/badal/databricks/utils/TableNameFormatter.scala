package io.badal.databricks.utils

object TableNameFormatter {
  def targetTableName(tableName: String): String = sanitizedTable(tableName)
  def logTableName(tableName: String): String =
    s"${sanitizedTable(tableName)}_log"
  def sanitizedTable(tableName: String): String = tableName.replace('.', '_')
}