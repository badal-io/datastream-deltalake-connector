#!/bin/bash

dbfs cp target/scala-2.12/datastream-deltalake-connector_2.12-0.1.0-SNAPSHOT.jar dbfs:/libs/
databricks libraries install --cluster-id $DB_CLUSTER_ID --jar target/scala-2.12/datastream-deltalake-connector_2.12-0.1.0-SNAPSHOT.jar
