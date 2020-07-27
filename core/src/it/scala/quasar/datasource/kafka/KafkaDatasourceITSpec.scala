/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.datasource.kafka

import slamdata.Predef._

import java.util.UUID

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.typelevel.jawn.AsyncParser

import argonaut.Argonaut._
import argonaut._
import JawnParser.facade
import jawnfs2._
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.kernel.instances.uuid._
import net.manub.embeddedkafka.{EmbeddedK, EmbeddedKafka, EmbeddedKafkaConfig}
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.datasource.LightweightDatasourceModule
import quasar.connector.datasource.LightweightDatasourceModule.DS
import quasar.connector.{ByteStore, DataFormat, QueryResult, ResourceError}
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead
import quasar.{NoopRateLimitUpdater, RateLimiter, RateLimiting, ScalarStages}

import scala.concurrent.ExecutionContext
import scala.sys

class KafkaDatasourceITSpec extends Specification with BeforeAfterAll {
  sequential

  import KafkaDatasourceITSpec._

  var embeddedK: EmbeddedK = _

  override def beforeAll(): Unit = {
    embeddedK = EmbeddedKafka.start()(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0))
    implicit val config: EmbeddedKafkaConfig = embeddedK.config
    implicit val stringSerializer: Serializer[String] = new StringSerializer

    EmbeddedKafka.createCustomTopic("valueOnly")
    EmbeddedKafka.createCustomTopic("keyAndValue")
    EmbeddedKafka.createCustomTopic("partitioned", partitions = 5)

    EmbeddedKafka.withProducer[String, String, Unit] { producer =>
      producer.send(new ProducerRecord("valueOnly", s"{ ${q("key")}: ${q("value")} }"))
      producer.send(new ProducerRecord("valueOnly", "[1, 2, 3]"))
      producer.send(new ProducerRecord("valueOnly", q("string")))

      producer.send(new ProducerRecord("keyAndValue", q("key"), q("value")))
      producer.send(new ProducerRecord("keyAndValue", "[1, 2, 3]", "true"))

      for (number <- 1 to 50) producer.send(new ProducerRecord("partitioned", s"""{ "number": $number }"""))
    }
  }

  "Datasource" >> {
    def baseConfig = Json(
      "bootstrapServers" := List(s"localhost:${embeddedK.config.kafkaPort}"),
      "groupId" := "precog",
      "format" := Json(
        "type" := "json",
        "variant" := "line-delimited",
        "precise" := false)
    )

    "reads value only topics" >> {
      def config =
        ("topics" := List("valueOnly", "partitioned")) ->:
        ("decoder" := Decoder.rawValue.asJson) ->:
        baseConfig

      evaluateTyped(config, "valueOnly").unsafeRunSync() must beLike {
        case Right(jss) => jss must_=== List(
          Json("key" := jString("value")),
          Json.array(jNumber(1), jNumber(2), jNumber(3)),
          jString("string"))
      }
    }

    "reads topic keys" >> {
      def config =
        ("topics" := List("keyAndValue")) ->:
          ("decoder" := Decoder.rawKey.asJson) ->:
          baseConfig

      evaluateTyped(config, "keyAndValue").unsafeRunSync() must beLike {
        case Right(jss) => jss must_=== List(jString("key"), Json.array(jNumber(1), jNumber(2), jNumber(3)))
      }
    }

    "reads topic values" >> {
      def config =
        ("topics" := List("keyAndValue")) ->:
          ("decoder" := Decoder.rawValue.asJson) ->:
          baseConfig

      evaluateTyped(config, "keyAndValue").unsafeRunSync() must beLike {
        case Right(jss) => jss must_=== List(jString("value"), jTrue)
      }
    }

    "reads partitioned topics" >> {
      def config =
        ("topics" := List("valueOnly", "partitioned")) ->:
        ("decoder" := Decoder.rawValue.asJson) ->:
        baseConfig

      val expected = (1 to 50).map(n => Json("number" := jNumber(n))).toList

      evaluateTyped(config, "partitioned").unsafeRunSync() must beLike {
        case Right(jss) => jss must containTheSameElementsAs(expected)
      }
    }
  }

  override def afterAll(): Unit = EmbeddedKafka.stop()

}

object KafkaDatasourceITSpec {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] = MonadError_.facet[IO](ResourceError.throwableP)

  implicit final class DatasourceOps(val ds: DS[IO]) extends scala.AnyVal {
    def evaluate(read: InterpretedRead[ResourcePath]): Resource[IO, QueryResult[IO]] =
      ds.loadFull(read) getOrElseF Resource.liftF(IO.raiseError(new RuntimeException("No batch loader!")))
  }

  val ldJson: DataFormat = DataFormat.ldjson
  val awJson: DataFormat = DataFormat.json

  def evaluateTyped(cfg: Json, name: String): IO[Either[InitializationError[Json], List[Json]]] =
    evaluateTyped(cfg, ResourcePath.root() / ResourceName(name))

  def evaluateTyped(cfg: Json, path: ResourcePath): IO[Either[InitializationError[Json], List[Json]]] = {
    useDatasource(cfg) { ds =>
      ds.evaluate(InterpretedRead(path, ScalarStages.Id)) use {
        case QueryResult.Typed(`ldJson`, bytes, ScalarStages.Id) =>
          bytes.chunks.parseJson[Json](AsyncParser.ValueStream).compile.toList

        case QueryResult.Typed(`awJson`, bytes, ScalarStages.Id) =>
          bytes.chunks.parseJson[Json](AsyncParser.UnwrapArray).compile.toList

        case QueryResult.Typed(format, bytes, ScalarStages.Id) =>
          IO.raiseError(new RuntimeException(s"Unknown format $format"))

        case query =>
          IO.raiseError(new RuntimeException(s"Unknown query $query"))
      }
    }
  }


  def useDatasource[A](cfg: Json)(f: DS[IO] => IO[A]): IO[Either[InitializationError[Json], A]] = {
    RateLimiter[IO, UUID](1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID]).flatMap(rl =>
      KafkaDatasourceModule.lightweightDatasource[IO, UUID](cfg, rl, ByteStore.void[IO]) use { r =>
        EitherT.fromEither[IO](r).semiflatMap(f).value
      })
  }
  def q(s: String): String = s""""$s""""
}

