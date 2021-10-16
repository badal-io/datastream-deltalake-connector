package io.badal.databricks.utils

object TableNameFormatter {
  def targetTableName(tableMetadata: TableMetadata) = sanitizedTable(tableMetadata)
  def logTableName(tableMetadata: TableMetadata) = s"${sanitizedTable(tableMetadata)}_log"


  private def sanitizedTable(tableMetadata: TableMetadata) = tableMetadata.table.replace('.', '_')
}
