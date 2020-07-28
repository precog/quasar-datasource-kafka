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
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer, consumerResource}

import scala.collection.immutable.SortedSet

/**
 * A [[Consumer]] that fetches all messages available in a topic up to the last message
 * at the time it starts running.
 */
class FullConsumer[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    settings: ConsumerSettings[F, K, V],
    decoder: RecordDecoder[F, K, V])
    extends Consumer[F] with Logging {

  import FullConsumer.topicPartitionOrder

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  override def fetch(topic: String): Resource[F, Stream[F, Byte]] = {
    consumerResource[F].using(settings) evalMap { consumer =>
      for {
        endOffsets <- assignNonEmptyPartitionsForTopic(consumer, topic)
      } yield {
        if (endOffsets.nonEmpty) takeUntilEndOffsets(consumer.partitionedStream, endOffsets).flatMap(decoder)
        else Stream.empty
      }
    }
  }

  /**
   * Assigns all non-empty partitions of a topic to this consumer, and returns a map of
   * end offsets for each one. The returned map will not contain offsets for empty partitions.
   */
  def assignNonEmptyPartitionsForTopic(
      consumer: KafkaConsumer[F, K, V],
      topic: String)
      : F[Map[TopicPartition, Long]] = {
    for {
      info <- consumer.partitionsFor(topic)
      topicPartitionSet = SortedSet(info.map(partitionInfoToTopicPartition): _*)
      _ <- F.delay(log.info(s"TopicPartition Set: $topicPartitionSet"))
      beginningOffsets <- consumer.beginningOffsets(topicPartitionSet)
      _ <- F.delay(log.debug(s"${beginningOffsets.size} beginning offsets: $beginningOffsets"))
      endOffsets <- consumer.endOffsets(topicPartitionSet)
      _ <- F.delay(log.debug(s"${endOffsets.size} end offsets: $endOffsets"))
      nonEmptyPartitions = topicPartitionSet.filterNot(tp => endOffsets(tp) == beginningOffsets(tp))
      _ <- NonEmptySet.fromSet(nonEmptyPartitions).fold(F.unit)(consumer.assign)
      _ <- F.delay(log.info(s"Assigned partitions ${nonEmptyPartitions.mkString(" ")}"))
    } yield endOffsets.filterKeys(nonEmptyPartitions.contains)
  }

  def partitionInfoToTopicPartition(info: PartitionInfo): TopicPartition =
    new TopicPartition(info.topic(), info.partition())

  /**
   * Emits all messages up to the last message of each partition given the map of end offsets.
   *
   * @param endOffsets Map of all _non-empty_ topic/partitions to their end offsets (offset of last message + 1)
   */
  def takeUntilEndOffsets(
      // TODO: it would make more sense to take a KafkaConsumer, but that can't be mocked on fs2-kafka 1.0.0.
      stream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]],
      endOffsets: Map[TopicPartition, Long])
      : Stream[F, CommittableConsumerRecord[F, K, V]] = {
    stream
      .take(endOffsets.size.toLong) // assumes no automatic assignment; needs a filter otherwise
      .map(_.takeThrough(isNotOffsetLimit(_, endOffsets)))
      .parJoin(endOffsets.size)
  }

  /**
   * True if there is at least one more record in the same topic/partition.
   *
   * Kafka API's `endOffsets` method returns the offset of the last available message plus one. Therefore,
   * we return false if the current record corresponds to the offset of the last available message, or it
   * is greater than that. The latter should not happen.
   *
   * @param committableRecord Last record read
   * @param offsets map returned by Kafka's `endOffsets(Collection&lt;TopicPartition&gt;)` method
   */
  def isNotOffsetLimit(
      committableRecord: CommittableConsumerRecord[F, K, V],
      offsets: Map[TopicPartition, Long])
      : Boolean = {
    val record = committableRecord.record
    val topic = record.topic
    val partition = record.partition
    val topicPartition = new TopicPartition(topic, partition)
    val end = offsets(topicPartition)
    log.trace(s"Read offset ${record.offset} / ${end - 1} from $topicPartition")
    record.offset < (end - 1)  // From endOffsets javadoc: "the offset of the last available message + 1"
  }
}

object FullConsumer {
  /** Arbitrary order required by SortedSet, which is required by NonEmptySet, which is used by the API. */
  implicit val topicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
      settings: ConsumerSettings[F, K, V],
      decoder: RecordDecoder[F, K, V])
      : Consumer[F] =
    new FullConsumer(settings, decoder)
}
