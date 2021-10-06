import sbt.Keys.libraryDependencies

lazy val commonSettings = Seq(
  organization := "io.badal",
  organizationName := "badal.io",
  mainClass := Some("io.badal.databricks.DatastreamDatabricksConnector"),
 // unmanagedBase := new java.io.File("/usr/local/lib/python3.8/site-packages/pyspark/jars"),

    libraryDependencies ++= Seq(
    "eu.timepit" %% "refined" % Versions.Refined,
    "eu.timepit" %% "refined-pureconfig" % Versions.Refined,
    "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test",
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.apache.spark" %% "spark-streaming" % Versions.Spark % Provided,
    "com.github.pureconfig" %% "pureconfig" % Versions.PureConfig,
    "com.google.http-client" % "google-http-client" % "1.40.0",
    "com.typesafe" % "config" % Versions.Typesafe,
    "io.delta" %% "delta-core" % Versions.Delta,
  ),
  resolvers ++= Seq(
  ),
)

lazy val root = Project(id = "datastream-deltalake-connector", base = file("."))
  .settings(commonSettings)
  .settings(testSettings)
  .settings(scalafmtSettings)
  .settings(assemblySettings)
  .enablePlugins(AssemblyPlugin)
  //.aggregate(spark)

//lazy val spark = Project(id = "spark", base = file("spark"))
//  .settings(commonSettings)
//  .settings(testSettings)
//  .settings(scalafmtSettings)
//  .settings(assemblySettings)
//  .enablePlugins(AssemblyPlugin)
//  .settings(name := "datastream-deltalake-spark-connector")

lazy val testSettings = Seq(
  fork in Test := true,
)

lazy val scalafmtSettings = Seq(scalafmtOnCompile := true)

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Xlint:_,-missing-interpolator",
  "-Xfatal-warnings",
  "-Xmacro-settings:materialize-derivations",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value.toLowerCase}-assembly-${version.value}.jar",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  test in assembly := {},
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
  )
)
