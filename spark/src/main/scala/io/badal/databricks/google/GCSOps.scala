package io.badal.databricks.google

import com.google.cloud.storage.StorageOptions
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

object GCSOps {

  val logger = Logger.getLogger(GCSOps.getClass)

  def list(bucketName: String, path: String): Set[String] = {
    logger.info(s"listing buckets at $bucketName under path $path")

    val storage = StorageOptions.getDefaultInstance.getService()
    val bucket = storage.get(bucketName)

    val blobs = bucket.list().getValues.asScala

    val targets = blobs
      .collect {
        case blob if blob.getName.startsWith(path) =>
          val trimmed =
            if (path.isEmpty) blob.getName
            else blob.getName.substring(path.length + 1)

          trimmed.split("/").headOption.getOrElse("")
      }
      .filter(_.nonEmpty)
      .toSet

    targets
  }

}
