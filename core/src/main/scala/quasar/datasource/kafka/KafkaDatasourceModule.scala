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

import argonaut.Argonaut._
import argonaut._
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import cats.kernel.Hash
import quasar.RateLimiting
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.connector.datasource.DatasourceModule.DS
import quasar.connector.datasource.{DatasourceModule, Reconfiguration}
import quasar.connector.{ByteStore, MonadResourceErr, ExternalCredentials}
import scalaz.NonEmptyList

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.{Long, Option}
import scala.util.{Either, Left, Right}

object KafkaDatasourceModule extends DatasourceModule {
  override def kind: DatasourceType = DatasourceType("kafka", 1L)

  override def sanitizeConfig(config: Json): Json = config.as[Config].result match {
    case Left(_) => config
    case Right(cfg) => cfg.sanitize.asJson
  }

  def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json): F[Either[DatasourceError.ConfigurationError[Json], Json]] =
    Sync[F].pure(Right(config))

  override def reconfigure(
    original: Json,
    patch: Json): Either[DatasourceError.ConfigurationError[Json], (Reconfiguration, Json)] = {
    val back = for {
      originalConfig <- original.as[Config].result.leftMap(_ =>
        DatasourceError
          .MalformedConfiguration[Json](
            kind,
            sanitizeConfig(original),
            "Source configuration in reconfiguration is malformed."))

      patchConfig <- patch.as[Config].result.leftMap(_ =>
        DatasourceError
          .MalformedConfiguration[Json](
            kind,
            sanitizeConfig(patch),
            "Patch configuration in reconfiguration is malformed."))

      reconfigured <- originalConfig.reconfigure(patchConfig).leftMap(c =>
        DatasourceError.InvalidConfiguration[Json](
          kind,
          c.asJson,
          NonEmptyList("Patch configuration contains sensitive information.")))
    } yield reconfigured.asJson

    back.tupleLeft(Reconfiguration.Reset)
  }

  override def datasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
    config: Json,
    rateLimiting: RateLimiting[F, A],
    byteStore: ByteStore[F],
    getAuth: UUID => F[Option[ExternalCredentials[F]]])
    (implicit ec: ExecutionContext): Resource[F, Either[DatasourceError.InitializationError[Json], DS[F]]] = {
    config.as[Config].result match {
      case Right(kafkaConfig) =>
        for {
          kafkaConsumerBuilder <- KafkaConsumerBuilder.resource(kafkaConfig, kafkaConfig.decoder)
          datasource <- Resource.pure[F, DS[F]](KafkaDatasource(kafkaConfig, kafkaConsumerBuilder))
        } yield datasource.asRight[DatasourceError.InitializationError[Json]]

      case Left((msg, _))  =>
        DatasourceError
          .invalidConfiguration[Json, InitializationError[Json]](kind, sanitizeConfig(config), NonEmptyList(msg))
          .asLeft[DatasourceModule.DS[F]]
          .pure[Resource[F, ?]]

    }
  }
}
