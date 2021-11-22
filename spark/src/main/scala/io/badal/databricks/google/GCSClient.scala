package io.badal.databricks.google

import com.google.api.gax.paging
import com.google.cloud.storage.{Blob, StorageOptions}

trait GCSClient {
  def list(bucketName: String): paging.Page[Blob]
}

object GCSClientImpl extends GCSClient {
  override def list(bucketName: String): paging.Page[Blob] = {
    val storage = StorageOptions.getDefaultInstance.getService()
    val bucket = storage.get(bucketName)

    bucket.list()
  }
}
