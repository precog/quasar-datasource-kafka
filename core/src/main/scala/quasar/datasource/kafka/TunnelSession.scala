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

import java.util.concurrent.atomic.AtomicReference

import com.jcraft.jsch.Session

final case class TunnelSession(session: Session) {
  private[this] val tunnelsRef: AtomicReference[List[((String, Int), Int)]] = new AtomicReference(Nil)
  def ports: List[Int] = tunnelsRef.get().map(_._2)
  def resolve(host: String, port: Int): Int = {
    var tunnels = tunnelsRef.get()
    var currentPort = tunnels.find((host, port) == _._1).map(_._2)
    while (currentPort.isEmpty) {
      val localPort = session.setPortForwardingL(0, host, port)
      if (tunnelsRef.compareAndSet(tunnels, ((host, port) -> localPort) :: tunnels)) {
        currentPort = Some(localPort)
      } else {
        session.delPortForwardingL(localPort)
        tunnels = tunnelsRef.get()
      }
    }
    currentPort.get
  }

  def hasTunnel(port: Int): Boolean = tunnelsRef.get().exists(port == _._2)
}
