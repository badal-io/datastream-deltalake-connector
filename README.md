# datastream-deltalake-connector

## Setup

### Building Fat Jar for the Spark connector

`sbt "project spark-connector" clean assembly`

### Connecting to GCP Locally

You need to download gcloud command line tools
Download and set up gcloud sdk (https://cloud.google.com/sdk/docs/install) and executing the following commands through CLI:

`gcloud auth application-default login`

May need to set your project configuration to the relevant one also,

`gcloud config set project sandbox-databricks`