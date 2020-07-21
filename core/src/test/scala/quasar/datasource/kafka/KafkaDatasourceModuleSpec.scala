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

import org.specs2.mutable.Specification

import argonaut.Argonaut._
import argonaut._
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.connector.datasource.Reconfiguration


class KafkaDatasourceModuleSpec extends Specification {

  "reconfiguration" >> {
    val ldjsonJ = Json(
      "type" := "json",
      "variant" := "line-delimited",
      "precise" := false)

    val source = Json(
      "bootstrapServers" := List("a.b.c.d:xyzzy"),
      "groupId" := "precog",
      "topics" := List("a", "b", "c"),
      "decoder" := Decoder.rawKey.asJson,
      "format" := ldjsonJ
    )

    val patch = Json(
      "bootstrapServers" := List("w.x.y.z:abcd"),
      "groupId" := "precog2",
      "topics" := List("topic"),
      "decoder" := Decoder.rawValue.asJson,
      "format" := ldjsonJ
    )

    val invalid = Json()

    "returns malformed error if patch can't be decoded" >> {
      val error = DatasourceError.MalformedConfiguration(
        DatasourceType("kafka", 1L),
        invalid,
        "Patch configuration in reconfiguration is malformed.")

      KafkaDatasourceModule.reconfigure(source, invalid) must beLeft(error)
    }

    "returns malformed error if source can't be decoded" >> {
      val error = DatasourceError.MalformedConfiguration(
        DatasourceType("kafka", 1L),
        invalid,
        "Source configuration in reconfiguration is malformed.")

      KafkaDatasourceModule.reconfigure(invalid, patch) must beLeft(error)
    }

    "replace entire config with patch when original has no sensitive information" >> {
      KafkaDatasourceModule.reconfigure(source, patch) must beRight((Reconfiguration.Reset, patch))
    }
  }
}
