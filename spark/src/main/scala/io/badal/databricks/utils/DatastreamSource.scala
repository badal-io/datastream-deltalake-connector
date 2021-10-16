package io.badal.databricks.utils

sealed trait DatastreamSource
object MySQL extends DatastreamSource
object Oracle extends DatastreamSource