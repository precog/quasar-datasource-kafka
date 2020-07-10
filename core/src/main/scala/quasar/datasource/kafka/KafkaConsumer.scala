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

import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.slf4s.Logging

import cats.Order
import cats.data.NonEmptySet
import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, consumerResource}

import scala.collection.immutable.SortedSet

class KafkaConsumer[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    settings: ConsumerSettings[F, K, V],
    decoder: RecordDecoder[F, K, V])
    extends Consumer[F] with Logging {

  import KafkaConsumer.topicPartitionOrder

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  override def fetch(topic: String): Resource[F, Stream[F, Byte]] = {
    consumerResource[F].using(settings) evalMap { consumer =>
      for {
        info <- consumer.partitionsFor(topic)
        topicPartitionSet = SortedSet(info.map(partitionInfoToTopicPartition): _*)
        _ <- F.delay(log.debug(s"TopicPartition Set: $topicPartitionSet"))
        _ <- NonEmptySet.fromSet(topicPartitionSet).fold(F.unit)(consumer.assign)
        _ <- F.delay(log.info(s"Assigned partitions from $topic"))
        endOffsets <- consumer.endOffsets(topicPartitionSet)
        _ <- F.delay(log.debug(s"${endOffsets.size} offsets: $endOffsets"))
      } yield consumer.partitionedStream
        .map(_.takeThrough(isOffsetLimit(_, endOffsets)))
        .parJoin(endOffsets.size)
        .flatMap(decoder)
    }
  }

  def isOffsetLimit(committableRecord: CommittableConsumerRecord[F, K, V], offsets: Map[TopicPartition, Long]): Boolean = {
    val record = committableRecord.record
    val topic = record.topic
    val partition = record.partition
    val topicPartition = new TopicPartition(topic, partition)
    val end = offsets(topicPartition)
    record.offset < end
  }

  def partitionInfoToTopicPartition(info: PartitionInfo): TopicPartition =
    new TopicPartition(info.topic(), info.partition())
}

object KafkaConsumer {
  implicit val topicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))


  def apply[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
      settings: ConsumerSettings[F, K, V],
      decoder: RecordDecoder[F, K, V])
      : Consumer[F] =
    new KafkaConsumer(settings, decoder)
}
