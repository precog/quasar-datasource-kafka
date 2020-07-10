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

import org.specs2.mutable.Specification

import argonaut.Argonaut._

class DecoderSpec extends Specification {

  "Decoder CodecJson" >> {
    "is case sensitive" >> {
      "\"rawkey\"".decode[Decoder] must beLeft
      "\"rawKey\"".decode[Decoder] must beLeft
      "\"RawKey\"".decode[Decoder] must beRight(Decoder.rawKey)
      "\"RAWKEY\"".decode[Decoder] must beLeft

      "\"rawvalue\"".decode[Decoder] must beLeft
      "\"rawValue\"".decode[Decoder] must beLeft
      "\"RawValue\"".decode[Decoder] must beRight(Decoder.rawValue)
      "\"RAWVALUE\"".decode[Decoder] must beLeft
    }

    "encodes as string" >> {
      Decoder.rawKey.asJson mustEqual "RawKey".asJson
      Decoder.rawValue.asJson mustEqual "RawValue".asJson
    }
  }

}
