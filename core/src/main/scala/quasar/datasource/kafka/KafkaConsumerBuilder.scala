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
import fs2.{Chunk, Stream}
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings}
import quasar.connector.MonadResourceErr

class KafkaConsumerBuilder[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr](
    config: Config,
    decoder: Decoder)
    extends ConsumerBuilder[F] {

  def mkFullConsumer: Resource[F, Consumer[F]] = {
    val F: ConcurrentEffect[F] = ConcurrentEffect[F]
    Resource.liftF(F.delay(f"${config.groupId}%s_${randomUuid().toString}%s")) map { groupId =>
      val recordDecoder = decoder match {
        case Decoder.RawKey => KafkaConsumerBuilder.RawKey[F]
        case Decoder.RawValue => KafkaConsumerBuilder.RawValue[F]
      }

      val consumerSettings = ConsumerSettings[F, Array[Byte], Array[Byte]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(config.bootstrapServers.toList.mkString(","))
      // .withGroupId(config.groupId)  // only for streaming consumers
      // .withBlocker(Blocker.liftExecutionContext(???))  // change if the default, single-threaded, blocker causes issues

      FullConsumer(consumerSettings, recordDecoder)
    }
  }
}

object KafkaConsumerBuilder {

  def apply[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr](
      config: Config,
      decoder: Decoder)
      : ConsumerBuilder[F] =
    new KafkaConsumerBuilder(config, decoder)


  def RawKey[F[_]]: RecordDecoder[F, Array[Byte], Array[Byte]] =
    (record: CommittableConsumerRecord[F, Array[Byte], Array[Byte]]) => Stream.chunk(Chunk.bytes(record.record.key))

  def RawValue[F[_]]: RecordDecoder[F, Array[Byte], Array[Byte]] =
    (record: CommittableConsumerRecord[F, Array[Byte], Array[Byte]]) => Stream.chunk(Chunk.bytes(record.record.value))
}
