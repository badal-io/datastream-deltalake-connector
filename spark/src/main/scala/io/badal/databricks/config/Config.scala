package io.badal.databricks.config

import eu.timepit.refined.types.string.NonEmptyString
import eu.timepit.refined.types.numeric.PosInt

import io.badal.databricks.datastream.TableProvider

final case class DatastreamDeltaConf(
    datastream: DatastreamConf,
    deltalake: DeltalakeConf,
    generateLogTable: Boolean,
    checkpointDir: String
)

final case class DatastreamConf(
    name: NonEmptyString,
    startDate: Option[NonEmptyString],
    fileReadConcurrency: PosInt,
    tableSource: TableProvider,
    readFormat: NonEmptyString
)

final case class DeltalakeConf(
    tableNamePrefix: String,
    mergeFrequencyMinutes: PosInt,
    schemaEvolution: SchemaEvolutionStrategy
)
