package io.badal.databricks
import com.google.api.client.util.DateTime

final case class DatastremJobConfig(
    inputBucket: String,
    dataStreamName: String,
    startDateTime: DateTime,
    fileReadConcurrency: Int,
    targetTableNamePrefix: String,
    mergeFrequencyMinutes: Int
)
