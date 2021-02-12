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

import java.nio.charset.Charset

import org.specs2.matcher.MatchResult

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.effect.{IO, Resource}
import fs2.{Chunk, Stream}
import quasar.ScalarStages
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector._
import quasar.connector.datasource.DatasourceSpec
import quasar.connector.datasource.LightweightDatasourceModule.DS
import quasar.datasource.kafka.TestImplicits._
import quasar.qscript.InterpretedRead
import shims.applicativeToScalaz

class KafkaDatasourceSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {
  import KafkaDatasourceSpec._

  override def datasource: Resource[IO, DS[IO]] = mkDatasource(config)

  override val nonExistentPath: ResourcePath = ResourcePath.root() / ResourceName("other")

  override def gatherMultiple[A](g: Stream[IO, A]): IO[List[A]] = g.compile.toList

  "BatchLoader.Full" >> {
    "read line delimited JSON topic a" >>* {
      assertResultBytes(
        datasource,
        ResourcePath.root() / ResourceName("a"),
        "abc".getBytes(Charset.forName("UTF-8")))
    }

    "read line delimited JSON topic b" >>* {
      assertResultBytes(
        datasource,
        ResourcePath.root() / ResourceName("b"),
        "bc".getBytes(Charset.forName("UTF-8")))
    }

    "read line delimited JSON topic c" >>* {
      assertResultBytes(
        datasource,
        ResourcePath.root() / ResourceName("c"),
        "c".getBytes(Charset.forName("UTF-8")))
    }

    "reading root raises ResourceError.NotAResource" >>* {
      val path = ResourcePath.root()
      val read = datasource.flatMap(_.loadFull(iRead(path)).value)

      MonadResourceErr[IO].attempt(read.use(_ => IO.unit)).map(_.toEither must beLeft.like {
        case ResourceError.NotAResource(_) => ok
      })
    }

    "reading a non-existent file raises ResourceError.PathNotFound" >>* {
      val path = ResourcePath.root() / ResourceName("does-not-exist")
      val read = datasource.flatMap(_.loadFull(iRead(path)).value)

      MonadResourceErr[IO].attempt(read.use(_ => IO.unit)).map(_.toEither must beLeft.like {
        case ResourceError.PathNotFound(_) => ok
      })
    }

  }

  "pathIsResource" >> {
    "the root of a bucket is not a resource" >>* {
      val root = ResourcePath.root()
      datasource.flatMap(_.pathIsResource(root)).use(b => IO.pure(b must beFalse))
    }
  }

  "prefixedChildPaths" >> {
    "list children at the root of the bucket" >>* {
      assertPrefixedChildPaths(
        ResourcePath.root(),
        List(
          ResourceName("a") -> ResourcePathType.leafResource,
          ResourceName("b") -> ResourcePathType.leafResource,
          ResourceName("c") -> ResourcePathType.leafResource))
    }
  }

  def assertPrefixedChildPaths(path: ResourcePath, expected: List[(ResourceName, ResourcePathType)]): IO[MatchResult[Any]] =
    OptionT(datasource.flatMap(_.prefixedChildPaths(path)))
      .getOrElseF(Resource.liftF(IO.raiseError(new Exception(s"Failed to list resources under $path"))))
      .use(gatherMultiple)
      .map(result => {
        // assert the same elements, with no duplicates
        result.length must_== expected.length
        result.toSet must_== expected.toSet
      })

  def iRead[A](path: A): InterpretedRead[A] = InterpretedRead(path, ScalarStages.Id)

  def assertResultBytes(ds: Resource[IO, DS[IO]], path: ResourcePath, expected: Array[Byte]): IO[MatchResult[Any]] =
    ds.flatMap(_.loadFull(iRead(path)).value) use {
      case Some(QueryResult.Typed(_, data, ScalarStages.Id)) =>
        data.data.compile.to(Array).map(_ must_=== expected)

      case _ =>
        IO(ko("Unexpected QueryResult"))
    }
}

object KafkaDatasourceSpec {
  def mkDatasource(config: Config): Resource[IO, DS[IO]] = {

    def mockConsumerBuilder[F[_]: Applicative]: ConsumerBuilder[F] = new ConsumerBuilder[F] {
      override def build(offsets: Offsets): Resource[F, Consumer[F]] = {
        Resource.pure[F, Consumer[F]] {
          (topic: String) => {
            val bytes = Stream.unfoldChunk(config.topics.toList.dropWhile(_ != topic)) {
              case h :: t => Some((Chunk.bytes(h.getBytes), t))
              case Nil    => None
            }
            Resource.pure[F, (Offsets, Stream[F, Byte])]((offsets, bytes))
          }
        }
      }
    }

    Resource.pure[IO, DS[IO]](KafkaDatasource(config, mockConsumerBuilder))
  }

  val config: Config = Config(
    bootstrapServers = NonEmptyList.of("a.b.c.d:xyzzy"),
    groupId = "group",
    topics = NonEmptyList.of("a", "b", "c"),
    decoder = Decoder.rawValue,
    tunnelConfig = None,
    format = DataFormat.ldjson)

}
