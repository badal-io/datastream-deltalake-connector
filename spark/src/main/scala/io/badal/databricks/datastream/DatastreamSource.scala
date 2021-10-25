package io.badal.databricks.datastream

sealed trait DatastreamSource
object MySQL extends DatastreamSource
object Oracle extends DatastreamSource
