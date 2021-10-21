package io.badal.databricks.config

import enumeratum._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrameWriter, Row}

sealed abstract class SchemaEvolutionStrategy(override val entryName: String)
    extends EnumEntry {
  def applyOption(writer: DataFrameWriter[Row]): DataFrameWriter[Row] =
    writer.option(entryName, true)
  def applyOption(writer: DataStreamWriter[Row]): DataStreamWriter[Row] =
    writer.option(entryName, true)
}

object SchemaEvolutionStrategy extends Enum[SchemaEvolutionStrategy] {
  val values = findValues

  case object Merge extends SchemaEvolutionStrategy("mergeSchema")
  case object Overwrite extends SchemaEvolutionStrategy("overwriteSchema")

  case object NoChanges extends SchemaEvolutionStrategy("none") {
    override def applyOption(
        writer: DataFrameWriter[Row]): DataFrameWriter[Row] = writer
    override def applyOption(
        writer: DataStreamWriter[Row]): DataStreamWriter[Row] = writer
  }

  class DataFrameWriterWithSchemaOptions(val writer: DataFrameWriter[Row]) {
    def option(schemaOption: SchemaEvolutionStrategy): DataFrameWriter[Row] =
      schemaOption.applyOption(writer)
  }

  class DataStreamWriterWithSchemaOptions(val writer: DataStreamWriter[Row]) {
    def option(schemaOption: SchemaEvolutionStrategy): DataStreamWriter[Row] =
      schemaOption.applyOption(writer)
  }

  implicit def writerConverter(
      writer: DataFrameWriter[Row]): DataFrameWriterWithSchemaOptions =
    new DataFrameWriterWithSchemaOptions(writer)

  implicit def streamWriterConverter(
      writer: DataStreamWriter[Row]): DataStreamWriterWithSchemaOptions =
    new DataStreamWriterWithSchemaOptions(writer)
}
