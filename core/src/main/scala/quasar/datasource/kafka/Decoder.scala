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

import argonaut._, Argonaut._

sealed trait Decoder

object Decoder {
  case object RawKey extends Decoder
  case object RawValue extends Decoder

  val rawKey: Decoder = RawKey
  val rawValue: Decoder = RawValue

  implicit val decoderEJ: EncodeJson[Decoder] = EncodeJson {
    case RawKey   => "RawKey".asJson
    case RawValue => "RawValue".asJson
  }

  implicit val decoderDJ: DecodeJson[Decoder] = c => c.as[String] flatMap {
    case "RawKey"   => DecodeResult.ok(RawKey)
    case "RawValue" => DecodeResult.ok(RawValue)
    case other      => DecodeResult.fail(s"Unknown decoder $other", c.history)
  }
}
