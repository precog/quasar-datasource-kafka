import scala.collection.Seq

ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-datasource-kafka"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-datasource-kafka"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-kafka"),
  "scm:git@github.com:precog/quasar-datasource-kafka.git"))

ThisBuild / publishAsOSSProject := true

ThisBuild / githubWorkflowBuildPreamble ++= Seq(
  WorkflowStep.Run(
    List(
      """ssh-keygen -t rsa -N "passphrase" -f key_for_docker""",
      "docker swarm init",
      "docker stack deploy -c docker-compose.yml teststack"),
    name = Some("Start zookeeper, kafka and sshd instances")),
  WorkflowStep.Run(
    List("it/scripts/run_test_data.sh"),
    name = Some("Load integration test data")))

lazy val IT = Tags.Tag("integrationTest")
Global / concurrentRestrictions += Tags.exclusive(IT)

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
  .aggregate(core, it)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-datasource-kafka",

    quasarPluginName := "url",

    quasarPluginQuasarVersion := quasarVersion.value,

    quasarPluginDatasourceFqcn := Some("quasar.datasource.kafka.KafkaDatasourceModule$"),

    quasarPluginDependencies ++= Seq(
      "com.github.fd4s"  %% "fs2-kafka"     % "1.0.0",
      "com.jcraft"       %  "jsch"          % "0.1.55",
      "org.apache.kafka" %  "kafka-clients" % "2.5.0",
      "org.slf4s"        %% "slf4s-api"     % "1.7.25"),

    libraryDependencies ++= Seq(
      "com.precog"              %% "quasar-foundation"    % quasarVersion.value % Test classifier "tests",
      "org.slf4j"               %  "slf4j-simple"         % "1.7.25"            % Test,
      "org.specs2"              %% "specs2-core"          % specs2Version       % Test,
      "org.specs2"              %% "specs2-matcher-extra" % specs2Version       % Test,
      "org.specs2"              %% "specs2-scalacheck"    % specs2Version       % Test,
      "org.specs2"              %% "specs2-scalaz"        % specs2Version       % Test))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_PATH", "connector", true)

lazy val it = project
  .in(file("it"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(noPublishSettings)
  .settings(
    name := "quasar-datasource-kafka-integration-test",

    // FIXME: This forces normal tests to run before integration tests, but we just want mutual exclusion
    Test / test := (Test/test tag IT).dependsOn(core/Test/test).value,

    Test / parallelExecution := false,

    libraryDependencies ++= Seq(
      "com.precog"              %% "quasar-foundation"    % quasarVersion.value % Test classifier "tests",
      "io.argonaut"             %% "argonaut-jawn"        % "6.3.0-M2"          % Test,
      "org.http4s"              %% "jawn-fs2"             % "1.0.0-RC2"         % Test,
      "org.slf4j"               %  "slf4j-simple"         % "1.7.25"            % Test,
      "org.specs2"              %% "specs2-core"          % specs2Version       % Test))
  .evictToLocal("QUASAR_PATH", "connector", true)
