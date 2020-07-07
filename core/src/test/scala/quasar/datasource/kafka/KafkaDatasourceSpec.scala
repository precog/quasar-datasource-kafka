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
import cats.effect.{IO, Resource}
import fs2.Stream
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.datasource.LightweightDatasourceModule.DS
import quasar.connector.datasource.{Datasource, DatasourceSpec}
import quasar.connector.{DataFormat, ResourceError}
import quasar.contrib.scalaz.MonadError_

class KafkaDatasourceSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {

  def mkDatasource(config: Config)
      : Resource[IO, DS[IO]] = {

    import quasar.datasource.kafka.KafkaDatasourceSpec._

//    implicit val timer = IO.timer(global)

    def mockConsumerBuilder[F[_]]: ConsumerBuilder[F] = new ConsumerBuilder[F] {
      override def mkConsumer: Resource[F, Consumer[F]] = ???
    }

    Resource.pure[IO, DS[IO]](KafkaDatasource(config, mockConsumerBuilder))
  }

  val config: Config = Config(
    bootstrapServers = NonEmptyList.of("a.b.c.d:xyzzy"),
    groupId = "group",
    topics = NonEmptyList.of("a", "b", "c"),
    decoder = Decoder.rawValue,
    format = DataFormat.ldjson)

  override def datasource: Resource[IO, Datasource[Resource[IO, *], Stream[IO, *], _, _, ResourcePathType.Physical]] =
    mkDatasource(config)

  override val nonExistentPath: ResourcePath = ResourcePath.root() / ResourceName("other")

  override def gatherMultiple[A](g: Stream[IO, A]): IO[List[A]] = g.compile.toList
}

object KafkaDatasourceSpec {
  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)
}
