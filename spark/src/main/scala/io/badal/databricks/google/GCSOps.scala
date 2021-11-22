package io.badal.databricks.google

import com.google.api.gax.paging
import com.google.cloud.storage.Blob
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object GCSOps {

  val logger = Logger.getLogger(GCSOps.getClass)

  def list(gcs: GCSClient, bucketName: String, path: String): Set[String] = {

    val safePath = if (path.endsWith("/") || path.isEmpty) path else s"$path/"

    @tailrec
    def listTailRec(acc: Set[String], page: paging.Page[Blob]): Set[String] = {
      val targets = acc ++ page.getValues.asScala
        .collect {
          case blob if blob.getName.startsWith(safePath) =>
            val trimmed =
              if (safePath.isEmpty) blob.getName
              else blob.getName.substring(path.length + 1)

            trimmed.split("/").headOption.getOrElse("")
        }
        .filter(_.nonEmpty)
        .toSet

      if (page.hasNextPage) {
        listTailRec(targets, page.getNextPage)
      } else {
        targets
      }
    }

    logger.info(s"listing buckets at $bucketName under path $path")

    val blobs = gcs.list(bucketName)

    listTailRec(Set.empty, blobs)
  }

}
