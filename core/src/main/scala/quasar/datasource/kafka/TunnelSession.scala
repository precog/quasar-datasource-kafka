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

/**
 * Manages query and creation of tunnels on the provided SSH session.
 */
final class TunnelSession private (session: Session) {
  private[this] val tunnelsRef: AtomicReference[List[((String, Int), Int)]] = new AtomicReference(Nil)

  /** All local ports of tunnels in this session. */
  def ports: List[Int] = tunnelsRef.get().map(_._2)

  /** True if `port` is the local port of an existing tunnel. */
  def hasTunnel(port: Int): Boolean = tunnelsRef.get().exists(port == _._2)

  /**
   * Returns the local port of a tunnel to `host:port`, creating the tunnel if none exists.
   *
   * Race conditions on multithreaded environment can lead to multiple tunnels being created at
   * the same time, to the same or different destinations. In such cases one tunnel "wins" and
   * the others are removed and then retried until successful.
   *
   * @param host Address on the remote network of this `session`.
   * @param port Port on the `host`.
   * @return Port on localhost.
   */
  @tailrec
  def resolve(host: String, port: Int): Int = {
    val tunnels: List[((String, Int), Int)] = tunnelsRef.get()
    tunnels.find((host, port) == _._1).map(_._2) match {
      case Some(port) => port
      case None       =>
        val localPort = session.setPortForwardingL(0, host, port)
        val isTunnelsRefUpdated = tryUpdateTunnelsRef(tunnels, host, port, localPort)
        if (isTunnelsRefUpdated) {
          localPort
        } else {
          session.delPortForwardingL(localPort)
          resolve(host, port)
        }
    }
  }

  /**
   * Prepend a new entry to `tunnels` and then try to update `tunnelsRef` with it.
   * If `tunnelsRef` current value is different than `tunnels`, then it does not get updated.
   *
   * @param tunnels List of existing tunnels.
   * @param host Remote host of a new tunnel.
   * @param port Remote port of a new tunnel.
   * @param localPort Local port of a new tunnel.
   * @return False if the current value of `tunnelsRef` is different than `tunnels`, true otherwise.
   */
  private[kafka] def tryUpdateTunnelsRef(
      tunnels: List[((String, Int), Int)],
      host: String,
      port: Int,
      localPort: Int): Boolean = {
    val updatedTunnels = ((host, port) -> localPort) :: tunnels
    tunnelsRef.compareAndSet(tunnels, updatedTunnels)
  }
}

object TunnelSession {
  def apply(session: Session): TunnelSession = new TunnelSession(session)
}
