import sbt.Keys.libraryDependencies

lazy val commonSettings = Seq(
  organization := "io.badal",
  organizationName := "badal.io",
  libraryDependencies ++= Seq(
    "eu.timepit" %% "refined" % Versions.Refined,
    "eu.timepit" %% "refined-pureconfig" % Versions.Refined,
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.apache.spark" %% "spark-core" % Versions.Spark % Provided,
    "org.apache.spark" %% "spark-streaming" % Versions.Spark % Provided,
    "com.github.pureconfig" %% "pureconfig" % Versions.PureConfig,
    "com.google.cloud" % "google-cloud-storage" % Versions.GoogleCloudStorgage,
    "com.typesafe" % "config" % Versions.Typesafe,
    "io.delta" %% "delta-core" % Versions.Delta,
    "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test",
    "org.apache.spark" %% "spark-sql" % Versions.Spark  % Test classifier "tests",
    "org.apache.spark" %% "spark-catalyst" % Versions.Spark  % Test classifier "tests",
    "org.apache.spark" %% "spark-core" % Versions.Spark  % Test classifier "tests",
    "org.apache.spark" %% "spark-hive" % Versions.Spark  % Test classifier "tests"
  ),
  resolvers ++= Seq(
  ),
)

lazy val root = Project(id = "datastream-deltalake-connector", base = file("."))
  .aggregate(spark)

lazy val spark = Project(id = "spark", base = file("spark"))
  .settings(commonSettings)
  .settings(testSettings)
  .settings(scalafmtSettings)
  .settings(assemblySettings)
  .enablePlugins(AssemblyPlugin)
  .settings(name := "datastream-deltalake-spark-connector",
    scalaSource in Test := baseDirectory.value / "src/test/scala",
    scalaSource in Compile := baseDirectory.value / "src/main/scala"
  )

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
  ),
  assemblyMergeStrategy in assembly := {
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
