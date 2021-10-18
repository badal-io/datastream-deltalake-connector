package io.badal.databricks.datastream

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.utils.GCSOps

sealed trait TableProvider {
  def list(): Seq[DatastreamTable]

}
final case class DiscoveryBucket(bucket: NonEmptyString,
                                 pathOpt: Option[NonEmptyString])
    extends TableProvider {
  override def list(): Seq[DatastreamTable] = {
    val path = pathOpt.map(_.value).getOrElse("")
    GCSOps
      .list(bucket.value, path)
      .toSeq
      .flatMap { tableDir =>
        tableDir.split("\\.").toList match {
          case List(database, table) =>
            Option(DatastreamTable(bucket.value, path, database, table))
          case _ =>
            // todo: log invalid directory
            None
        }
      }
  }
}
