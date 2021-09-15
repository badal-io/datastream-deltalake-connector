name := "datastream-databricks-connector"
scalaVersion := "2.12.10"

fork in Test := true

val SPARK_VERSION = "3.1.1"
val DELTA_VERSION = "0.8.0"


libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.0"
//libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % SPARK_VERSION
libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION
libraryDependencies += "org.apache.spark" %% "spark-streaming" % SPARK_VERSION
libraryDependencies += "com.google.http-client" % "google-http-client" % "1.39.2"
libraryDependencies += "com.google.http-client" % "google-http-client-test" % "1.40.0" % Test
libraryDependencies += "io.delta" %% "delta-core" % DELTA_VERSION
