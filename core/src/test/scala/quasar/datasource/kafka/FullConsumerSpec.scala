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

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.specs2.concurrent.ExecutionEnv
import org.specs2.matcher.TerminationMatchers
import org.specs2.mutable.Specification

import cats.effect._
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ConsumerRecord, ConsumerSettings}
import quasar.datasource.kafka.TestImplicits._

import scala.concurrent.duration._

class FullConsumerSpec(implicit ec: ExecutionEnv) extends Specification with TerminationMatchers {

  "assignTopic" >> {
    "assigns all non empty partitions" >> {
      todo
    }

    "does not assign empty partitions" >> {
      todo
    }

    "returns end offsets of all non empty partitions" >> {
      todo
    }

    "does not return end offset of empty partitions" >> {
      todo
    }
  }

  "limitStream" >> {
    val settings = ConsumerSettings[IO, Array[Byte], Array[Byte]]
    val kafkaConsumer = new FullConsumer[IO, Array[Byte], Array[Byte]](settings, KafkaConsumerBuilder.RawKey)

    "terminates stream once data from the sole substream is read" >> {
      val tp = new TopicPartition("topic", 0)
      val offset = 5L
      val endOffsets = Map(tp -> (offset + 1L))
      val mkRecord = mkCommittableConsumerRecord(tp, (_: Long), "key" -> "value")
      val assignment = IO.pure(Stream.iterate[IO, Long](offset)(_ + 1).map(mkRecord))
      val stream = Stream.eval(assignment)

      kafkaConsumer.takeUntilEndOffsets(stream, endOffsets).compile.drain.unsafeRunSync() must terminate(sleep = 2.seconds)
    }

    "terminates stream even if main stream keeps producing auto assignments" >> {
      val tp = new TopicPartition("topic", 0)
      val offset = 5L
      val endOffsets = Map(tp -> (offset + 1L))
      val mkRecord = mkCommittableConsumerRecord(tp, (_: Long), "key" -> "value")
      val assignment = IO.pure(Stream.iterate[IO, Long](offset)(_ + 1).map(mkRecord))
      val stream = Stream.eval(assignment).repeat // repeat to emulate hypothetical automatic assignments

      kafkaConsumer.takeUntilEndOffsets(stream, endOffsets).compile.drain.unsafeRunSync() must terminate(sleep = 2.seconds)
    }
  }

  "isOffsetLimit" >> {
    val settings = ConsumerSettings[IO, Array[Byte], Array[Byte]]
    val kafkaConsumer = new FullConsumer[IO, Array[Byte], Array[Byte]](settings, KafkaConsumerBuilder.RawKey)
    val tp1 = new TopicPartition("precog", 0)
    val tp2 = new TopicPartition("precog", 1)
    val tp3 = new TopicPartition("topic", 0)
    val endOffsets = Map(tp1 -> 100L, tp2 -> 20L, tp3 -> 30L)
    val entry: (String, String) = "key" -> "value"

    // TODO: test that isNotOffsetLimit can be used with `takeThrough`, instead of testing the values it returns

    "is true if record offset is less than end offset - 1" >> {
      val record = mkCommittableConsumerRecord(tp1, 5L, entry)
      kafkaConsumer.isNotOffsetLimit(record, endOffsets) must beTrue
    }

    "is false if record offset is equal to end offset - 1" >> {
      val record = mkCommittableConsumerRecord(tp2, 19L, entry)
      kafkaConsumer.isNotOffsetLimit(record, endOffsets) must beFalse
    }

    "is true if record offset is more than end offset - 1" >> {
      val record = mkCommittableConsumerRecord(tp3, 50L, entry)
      kafkaConsumer.isNotOffsetLimit(record, endOffsets) must beFalse
    }
  }

  def mkCommittableConsumerRecord(tp: TopicPartition, offset: Long, entry: (String, String))
      : CommittableConsumerRecord[IO, Array[Byte], Array[Byte]] =
    CommittableConsumerRecord[IO, Array[Byte], Array[Byte]](
      ConsumerRecord(tp.topic(), tp.partition(), offset, entry._1.getBytes, entry._2.getBytes),
      CommittableOffset(tp, new OffsetAndMetadata(offset), None, _ => IO.unit))

  def mkCommittableConsumerRecord(topic: String, partition: Int, offset: Long, key: String, value: String)
      : CommittableConsumerRecord[IO, Array[Byte], Array[Byte]] =
    mkCommittableConsumerRecord(new TopicPartition(topic, partition), offset, key -> value)

}
