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
import org.specs2.mutable.Specification

import cats.effect._
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ConsumerRecord, ConsumerSettings}

import scala.concurrent.ExecutionContext.global

class KafkaConsumerSpec extends Specification {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)

  "isOffsetLimit" >> {
    val settings = ConsumerSettings[IO, Array[Byte], Array[Byte]]
    val kafkaConsumer = new KafkaConsumer[IO, Array[Byte], Array[Byte]](settings, KafkaConsumerBuilder.RawKey)
    val tp1 = new TopicPartition("precog", 0)
    val tp2 = new TopicPartition("precog", 1)
    val tp3 = new TopicPartition("topic", 0)
    val endOffsets = Map(tp1 -> 100L, tp2 -> 20L, tp3 -> 30L)

    "is false if record offset is less than end offset" >> {
      val record = CommittableConsumerRecord[IO, Array[Byte], Array[Byte]](
        ConsumerRecord("precog", 0, 5, "key".getBytes, "value".getBytes),
        CommittableOffset(new TopicPartition("precog", 0), new OffsetAndMetadata(5), None, _ => IO.unit))
      kafkaConsumer.isOffsetLimit(record, endOffsets) must beFalse
    }

    "is true if record offset is equal to end offset" >> {
      val record = CommittableConsumerRecord[IO, Array[Byte], Array[Byte]](
        ConsumerRecord("precog", 1, 20, "key".getBytes, "value".getBytes),
        CommittableOffset(new TopicPartition("precog", 1), new OffsetAndMetadata(20), None, _ => IO.unit))
      kafkaConsumer.isOffsetLimit(record, endOffsets)
    }

    "is true if record offset is more than end offset" >> {
      val record = CommittableConsumerRecord[IO, Array[Byte], Array[Byte]](
        ConsumerRecord("topic", 0, 50, "key".getBytes, "value".getBytes),
        CommittableOffset(new TopicPartition("topic", 50), new OffsetAndMetadata(5), None, _ => IO.unit))
      kafkaConsumer.isOffsetLimit(record, endOffsets)
    }
  }

}
