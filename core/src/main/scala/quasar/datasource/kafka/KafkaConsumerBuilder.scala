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

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import fs2.kafka.ConsumerSettings
import quasar.connector.MonadResourceErr

class KafkaConsumerBuilder[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr, K, V](
    config: Config,
    consumerSettings: ConsumerSettings[F, K, V],
    decoder: RecordDecoder[F, K, V])
    extends ConsumerBuilder[F] {

  def mkConsumer: Resource[F, Consumer[F]] = {
    val F: ConcurrentEffect[F] = ConcurrentEffect[F]
    Resource.liftF(F.delay(f"${config.groupId}%s_${randomUuid().toString}%s")) map { groupId =>
      val settings = consumerSettings.withGroupId(groupId)
      KafkaConsumer(settings, decoder)
    }
  }
}

object KafkaConsumerBuilder {

  def apply[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr, K, V](
      config: Config,
      consumerSettings: ConsumerSettings[F, K, V],
      decoder: RecordDecoder[F, K, V])
      : ConsumerBuilder[F] =
    new KafkaConsumerBuilder(config, consumerSettings, decoder)
}
