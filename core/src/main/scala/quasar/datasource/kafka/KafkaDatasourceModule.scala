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

import argonaut.Argonaut._
import argonaut._
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._
import cats.kernel.Hash
import eu.timepit.refined.auto._
import fs2.kafka._
import quasar.RateLimiting
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.connector.datasource.LightweightDatasourceModule.DS
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}
import quasar.connector.{ByteStore, MonadResourceErr}
import scalaz.NonEmptyList

import scala.concurrent.ExecutionContext
import scala.util.{Either, Left, Right}

object KafkaDatasourceModule extends LightweightDatasourceModule {
  override def kind: DatasourceType = DatasourceType("kafka", 1L)

  override def sanitizeConfig(config: Json): Json = config.as[Config].result match {
    case Left(_) => config
    case Right(cfg) => cfg.sanitize.asJson
  }

  override def reconfigure(
    original: Json,
    patch: Json): Either[DatasourceError.ConfigurationError[Json], (Reconfiguration, Json)] = {
    val back = for {
      original <- original.as[Config].result.leftMap(_ =>
        DatasourceError
          .MalformedConfiguration[Json](
            kind,
            sanitizeConfig(original),
            "Source configuration in reconfiguration is malformed."))

      patch <- patch.as[Config].result.leftMap(_ =>
        DatasourceError
          .MalformedConfiguration[Json](
            kind,
            sanitizeConfig(patch),
            "Patch configuration in reconfiguration is malformed."))

      reconfigured <- original.reconfigure(patch).leftMap(c =>
        DatasourceError.InvalidConfiguration[Json](
          kind,
          c.asJson,
          NonEmptyList("Patch configuration contains sensitive information.")))
    } yield reconfigured.asJson

    back.tupleLeft(Reconfiguration.Reset)
  }

  override def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
    config: Json,
    rateLimiting: RateLimiting[F, A],
    byteStore: ByteStore[F])
    (implicit ec: ExecutionContext): Resource[F, Either[DatasourceError.InitializationError[Json], DS[F]]] = {
    config.as[Config].result match {
      case Right(kafkaConfig) =>
        val consumerSettings = ConsumerSettings[F, Array[Byte], Array[Byte]]
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withBootstrapServers(kafkaConfig.bootstrapServers.toList.mkString(","))
//          .withBlocker(Blocker.liftExecutionContext(ec))

        val decoder = kafkaConfig.decoder match {
          case Decoder.RawKey => Consumer.RawKey[F]
          case Decoder.RawValue => Consumer.RawValue[F]
        }

        Resource.pure[F, Either[DatasourceError.InitializationError[Json], DS[F]]](
          KafkaDatasource(kafkaConfig, consumerSettings, decoder).asRight)

      case Left((msg, _))  =>
        DatasourceError
          .invalidConfiguration[Json, InitializationError[Json]](kind, sanitizeConfig(config), NonEmptyList(msg))
          .asLeft[LightweightDatasourceModule.DS[F]]
          .pure[Resource[F, ?]]

    }
  }
}
