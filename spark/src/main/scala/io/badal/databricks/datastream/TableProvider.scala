package io.badal.databricks.datastream

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.utils.GCSOps

sealed trait TableProvider {
  def list(): Seq[DatastreamTable]

}
final case class DiscoveryBucket(bucket: NonEmptyString,
                                 path: Option[NonEmptyString])
    extends TableProvider {
  override def list(): Seq[DatastreamTable] = {
    val pathOrEmpty = path.map(_.value).getOrElse("")
    GCSOps
      .list(bucket.value, pathOrEmpty)
      .toSeq
      .map(table => DatastreamTable(bucket.value, pathOrEmpty, table))
  }
}
