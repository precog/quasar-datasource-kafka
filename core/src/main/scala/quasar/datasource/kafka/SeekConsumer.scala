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
import fs2.{Pipe, Pull, Stream}
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer, consumerResource}

import scala.collection.immutable.SortedSet

/**
 * A [[Consumer]] that fetches all messages available in a topic up to the last message
 * at the time it starts running.
 */
class SeekConsumer[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    initialOffsets: Offsets,
    settings: ConsumerSettings[F, K, V],
    decoder: RecordDecoder[F, K, V])
    extends Consumer[F] with Logging {

  import SeekConsumer.topicPartitionOrder

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  override def fetch(topic: String): Resource[F, (Offsets, Stream[F, Byte])] = {
    consumerResource[F].using(settings) evalMap { consumer =>
      for {
        endOffsets <- assignNonEmptyPartitionsForTopic(consumer, topic)
        _ <- endOffsets.toList.traverse_ { case (p, end) =>
          // See `offsetPull` comments why `w - 1`
          initialOffsets.get(p.partition()).traverse_(w => consumer.seek(p, w - 1))
        }
      } yield {
        val resultStream = if (endOffsets.nonEmpty) {
          takeUntilEndOffsets(consumer.partitionedStream, endOffsets).flatMap(decoder)
        }
        else {
          Stream.empty
        }
        val offsets = endOffsets.toList.map({case (k, v) => (k.partition(), v)}).toMap
        (offsets, resultStream)
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

  type CCR = CommittableConsumerRecord[F, K, V]

  /**
   * Emits all messages up to the last message of each partition given the map of end offsets.
   *
   * @param endOffsets Map of all _non-empty_ topic/partitions to their end offsets (offset of last message + 1)
   */
  def takeUntilEndOffsets(
      stream: Stream[F, Stream[F, CCR]],
      endOffsets: Map[TopicPartition, Long])
      : Stream[F, CCR] =
    stream
      .take(endOffsets.size.toLong)
      .map(_.through(offsetPipe(endOffsets)))
      .parJoin(endOffsets.size)

  // Can't use `filter` since we need to stop stream from execution if no new records appeared
  def offsetPipe(endOffsets: Map[TopicPartition, Long]): Pipe[F, CCR, CCR] =
    stream => offsetPull(stream, endOffsets).stream

  private def offsetPull(input: Stream[F, CCR], endOffsets: Map[TopicPartition, Long]): Pull[F, CCR, Unit] =
    // if we `consumer.seek` to previous end and there is no new events, then this waits until new messages
    // appear, that's why instead of `consumer.seek(p, w)` we do `consumer.seek(p, w - 1)`
    input.pull.uncons1 flatMap {
      case None =>
        Pull.done
      case Some((ccr, tail)) =>
        val record = ccr.record
        val topic = record.topic
        val partition = record.partition
        val topicPartition = new TopicPartition(topic, partition)
        val previous = initialOffsets.get(partition).getOrElse(0L) - 1
        // From endOffsets javadoc: "the offset of the last available message + 1"
        val end = endOffsets(topicPartition) - 1
        // No new events since previous push
        if (end <= previous) {
          log.trace(s"$topicPartition has no new element, previous offset: $previous")
          Pull.done
        }
        // This is the very first record from we should ignore it, since it was already consumed by
        // previous push.
        else if (record.offset <= previous) {
          log.trace(s"First Read offset is ignored ${record.offset} / $end from $topicPartition")
          offsetPull(tail, endOffsets)
        }
        // New events are here, and they're as new as our offset request
        else if (record.offset < end) {
          log.trace(s"Read offset ${record.offset} / $end from $topicPartition")
          Pull.output1(ccr) >> offsetPull(tail, endOffsets)
        }
        else if (record.offset === end) {
          log.trace(s"Last Read offset ${record.offset} / $end from $topicPartition stop the stream")
          // We can't just recurse here, since when there is no new messages, the stream waits for them
          Pull.output1(ccr) >> Pull.done
        }
        // New events are here, but they're newer than end offset request
        else {
          Pull.done
        }
    }
}

object SeekConsumer {
  /** Arbitrary order required by SortedSet, which is required by NonEmptySet, which is used by the API. */
  implicit val topicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
      initialOffsets: Offsets,
      settings: ConsumerSettings[F, K, V],
      decoder: RecordDecoder[F, K, V])
      : Consumer[F] =
    new SeekConsumer(initialOffsets, settings, decoder)
}
