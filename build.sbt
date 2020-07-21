import scala.collection.Seq

ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-datasource-kafka"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-datasource-kafka"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-kafka"),
  "scm:git@github.com:precog/quasar-datasource-kafka.git"))

ThisBuild / publishAsOSSProject := true

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val quasarVersion = Def.setting[String](
  managedVersions.value("precog-quasar"))

val specs2Version = "4.8.3"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .settings(name := "quasar-datasource-kafka-root")
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-datasource-kafka",

    quasarPluginName := "url",

    quasarPluginQuasarVersion := quasarVersion.value,

    quasarPluginDatasourceFqcn := Some("quasar.datasource.kafka.KafkaDatasourceModule$"),

    quasarPluginDependencies ++= Seq(
      "com.github.fd4s"  %% "fs2-kafka"     % "1.0.0",
      "org.apache.kafka" %  "kafka-clients" % "2.5.0",
      "org.slf4s"        %% "slf4s-api"     % "1.7.25"),

    libraryDependencies ++= Seq(
      "com.precog"              %% "quasar-foundation"    % quasarVersion.value % Test classifier "tests",
      "io.github.embeddedkafka" %% "embedded-kafka"       % "2.5.0"             % Test,
      "org.slf4j"               %  "slf4j-simple"         % "1.7.25"            % Test,
      "org.specs2"              %% "specs2-core"          % specs2Version       % Test,
      "org.specs2"              %% "specs2-matcher-extra" % specs2Version       % Test,
      "org.specs2"              %% "specs2-scalacheck"    % specs2Version       % Test,
      "org.specs2"              %% "specs2-scalaz"        % specs2Version       % Test))
  .enablePlugins(QuasarPlugin)
