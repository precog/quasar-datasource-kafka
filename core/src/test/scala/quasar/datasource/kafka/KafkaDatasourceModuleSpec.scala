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
import quasar.api.datasource.DatasourceError.InvalidConfiguration
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.connector.datasource.Reconfiguration
import scalaz.NonEmptyList


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
      "tunnelConfig" := Json(
        "user" := "user",
        "host" := "host",
        "port" := 22,
        "auth" := Json("password" := "secret")
      ),
      "decoder" := Decoder.rawKey.asJson,
      "format" := ldjsonJ
    )

    val patch = Json(
      "bootstrapServers" := List("w.x.y.z:abcd"),
      "groupId" := "precog2",
      "topics" := List("topic"),
      "decoder" := Decoder.rawValue.asJson,
      "tunnelConfig" := Json(
        "user" := "other",
        "host" := "server",
        "port" := 22222,
        "auth" := jNull
      ),
      "format" := ldjsonJ
    )

    val expected = Json(
      "bootstrapServers" := List("w.x.y.z:abcd"),
      "groupId" := "precog2",
      "topics" := List("topic"),
      "decoder" := Decoder.rawValue.asJson,
      "tunnelConfig" := Json(
        "user" := "other",
        "host" := "server",
        "port" := 22222,
        "auth" := Json("password" := "secret")
      ),
      "format" := ldjsonJ
    )

    val invalid = Json()

    val sensitive = Json(
      "bootstrapServers" := List("w.x.y.z:abcd"),
      "groupId" := "precog2",
      "topics" := List("topic"),
      "decoder" := Decoder.rawValue.asJson,
      "tunnelConfig" := Json(
        "user" := "other",
        "host" := "server",
        "port" := 22222,
        "auth" := Json("prv" := "private_key", "passphrase" := "secret")
      ),
      "format" := ldjsonJ
    )

    val sensitiveSanitized = Json(
      "bootstrapServers" := List("w.x.y.z:abcd"),
      "groupId" := "precog2",
      "topics" := List("topic"),
      "decoder" := Decoder.rawValue.asJson,
      "tunnelConfig" := Json(
        "user" := "other",
        "host" := "server",
        "port" := 22222,
        "auth" := Json("prv" := "<REDACTED>", "passphrase" := "<REDACTED>")
      ),
      "format" := ldjsonJ
    )

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

    "replaces non-sensitive information and keeps sensitive information" >> {
      KafkaDatasourceModule.reconfigure(source, patch) must beRight((Reconfiguration.Reset, expected))
    }

    "returns invalid configuration error if patch has sensitive information" >> {
      KafkaDatasourceModule.reconfigure(source, sensitive) must beLeft(
        InvalidConfiguration(
          KafkaDatasourceModule.kind,
          sensitiveSanitized,
          NonEmptyList("Patch configuration contains sensitive information.")
        )
      )
    }
  }
}
