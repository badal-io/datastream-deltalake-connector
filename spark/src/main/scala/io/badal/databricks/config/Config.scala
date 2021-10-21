package io.badal.databricks.config

import eu.timepit.refined.types.string.NonEmptyString
import eu.timepit.refined.types.numeric.PosInt
import io.badal.databricks.datastream.TableProvider

final case class DatastreamJobConf(datastream: DatastreamConf,
                                   deltalake: DeltalakeConf,
                                   generateLogTable: Boolean,
                                   checkpointDir: String)

final case class DatastreamConf(
    name: NonEmptyString,
    startDate: Option[NonEmptyString],
    fileReadConcurrency: PosInt,
    tableSource: TableProvider
)

final case class DeltalakeConf(tableNamePrefix: String,
                               mergeFrequencyMinutes: PosInt,
                               schemaEvolution: SchemaEvolutionStrategy)
//                               schemaEvolution: SchemaEvolution)
