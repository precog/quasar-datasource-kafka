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

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import fs2.Stream

import quasar.ScalarStages
import quasar.api.datasource.DatasourceType
import quasar.api.push.ExternalOffsetKey
import quasar.api.resource.ResourcePath.Root
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.datasource.{BatchLoader, LightweightDatasource, Loader}
import quasar.connector.{MonadResourceErr, QueryResult, ResourceError, ResultData, Offset}
import quasar.qscript.InterpretedRead

import scodec.{Attempt, Codec}
import scodec.bits.ByteVector
import scodec.codecs.{int32, int64, listOfN}

final class KafkaDatasource[F[_]: Concurrent: MonadResourceErr](
    config: Config,
    consumerBuilder: ConsumerBuilder[F])
    extends LightweightDatasource[Resource[F, ?], Stream[F, ?], QueryResult[F]]  {

  override def kind: DatasourceType = KafkaDatasourceModule.kind

  override def loaders: NonEmptyList[Loader[Resource[F, *], InterpretedRead[ResourcePath], QueryResult[F]]] =
    NonEmptyList.of(Loader.Batch(
      BatchLoader.Seek { (iRead, offset) =>
        Resource.liftF(extractTopic(iRead)).flatMap { case (topic, stages) =>
          seekConsumer(topic, stages, iRead.path, offset)
        }
      }))

  def extractTopic(iRead: InterpretedRead[ResourcePath]): F[(String, ScalarStages)] =
    iRead.path.unsnoc match {
      case Some(Root -> ResourceName(topic)) if config.isTopic(topic) =>
        ((topic, iRead.stages)).pure[F]
      case None =>
        MonadResourceErr[F].raiseError(ResourceError.NotAResource(iRead.path))
      case _ =>
        MonadResourceErr[F].raiseError(ResourceError.PathNotFound(iRead.path))
    }

  private def seekConsumer(
      topic: String,
      stages: ScalarStages,
      path: ResourcePath,
      offset: Option[Offset])
      : Resource[F, QueryResult[F]] = for {
    offsetMap <- Resource.liftF {
      offset.traverse(x => getEncodedOffsets(path, x).flatMap(decodeToMap(path, _)))
    }
    consumer <- consumerBuilder.build(offsetMap.getOrElse(Map.empty))
    (offsets, bytes) <- consumer.fetch(topic)
    offsetBytes <- Resource.liftF(encodeOffsets(path, offsets))
  } yield {
    val chunked = bytes.chunks.map(Right(_))
    val offseted = Stream.emit(Left(ExternalOffsetKey(offsetBytes)))
    val resultData = ResultData.Delimited(chunked ++ offseted)
    QueryResult.typed(config.format, resultData, stages)
  }

  private def getEncodedOffsets(path: ResourcePath, offset: Offset): F[Array[Byte]] = offset match {
    case Offset.External(ek) => ek.value.pure[F]
    case _ => MonadResourceErr[F].raiseError(ResourceError.seekFailed(path, "Kafka offset key must be external"))
  }

  private def encodeOffsets(path: ResourcePath, offs: Offsets): F[Array[Byte]] = mapCodec.encode(offs) match {
    case Attempt.Successful(bv) =>
      bv.toByteArray.pure[F]
    case _ =>
      MonadResourceErr[F].raiseError(ResourceError.seekFailed(path, "An error occured while encoding offsets"))
  }

  private def decodeToMap(path: ResourcePath, bytes: Array[Byte]): F[Offsets] =
    mapCodec.decode(ByteVector(bytes).toBitVector) match {
      case Attempt.Successful(s) =>
        s.value.pure[F]
      case _ =>
        MonadResourceErr[F].raiseError(ResourceError.seekFailed(path, "Kafka offset ExternalKey is incorrect"))
    }

  override def pathIsResource(path: ResourcePath): Resource[F, Boolean] =
    Resource.liftF(path.unsnoc match {
      case Some(Root -> ResourceName(topic)) =>
        config.isTopic(topic).pure[F]
      case _ =>
        false.pure[F]
    })

  override def prefixedChildPaths(prefixPath: ResourcePath)
      : Resource[F, Option[Stream[F, (ResourceName, ResourcePathType.Physical)]]] =
    pathIsResource(prefixPath) evalMap {
      case true =>
        Stream.empty
          .covaryOutput[(ResourceName, ResourcePathType.Physical)]
          .covary[F].some.pure[F]
      case false =>
        prefixPath match {
          case Root =>
            Stream(config.topics.toList: _*)
              .map(ResourceName(_) -> ResourcePathType.leafResource)
              .covaryOutput[(ResourceName, ResourcePathType.Physical)]
              .covary[F].some.pure[F]
          case _    =>
            none[Stream[F, (ResourceName, ResourcePathType.Physical)]].pure[F]
        }
    }

  implicit def mapCodec: Codec[Offsets] = listOfN(int32, int32 ~ int64).xmap(_.toMap, _.toList)
}

object KafkaDatasource {
  def apply[F[_]: Concurrent: MonadResourceErr](
      config: Config,
      consumerBuilder: ConsumerBuilder[F])
      : KafkaDatasource[F] =
    new KafkaDatasource(config, consumerBuilder)
}
