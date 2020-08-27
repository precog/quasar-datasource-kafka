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

final case class TunnelConfig(
  host: String,
  port: Int,
  user: String,
  auth: Option[TunnelConfig.Auth])
  extends Product with Serializable {
  import TunnelConfig._
  import TunnelConfig.Auth._
  def sanitize: TunnelConfig = {
    val newPass: Option[Auth] = auth map {
      case Password(_) => Password("<REDACTED>")
      case Identity(_, _) => Identity("<REDACTED>", Some("<REDACTED>"))
    }
    copy(auth = newPass)
  }
}

object TunnelConfig {
  implicit val codecTunnelConfig: CodecJson[TunnelConfig] =
    casecodec4(TunnelConfig.apply, TunnelConfig.unapply)("host", "port", "user", "auth")

  sealed trait Auth

  object Auth {
    final case class Identity(prv: String, passphrase: Option[String]) extends Auth
    final case class Password(password: String) extends Auth

    implicit val codecIdentity: CodecJson[Identity] =
      casecodec2(Identity.apply, Identity.unapply)("prv", "passphrase")

    implicit val codecPassword: CodecJson[Password] =
      casecodec1(Password.apply, Password.unapply)("password")

    implicit val codecTunnelPass: CodecJson[Auth] = CodecJson({
      case c: Identity => codecIdentity(c)
      case c: Password => codecPassword(c)
    }, { j: HCursor =>
      val id = codecIdentity(j).map(a => a: Auth)
      id.result.fold(_ => codecPassword(j).map(a => a: Auth), _ => id)
    })
  }
}
