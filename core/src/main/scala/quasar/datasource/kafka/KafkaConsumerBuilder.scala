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

import java.net.UnknownHostException

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.data.NonEmptyList
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.jcraft.jsch._
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings}
import fs2.{Chunk, Stream}
import quasar.connector.MonadResourceErr
import quasar.datasource.kafka.KafkaConsumerBuilder.TunnelSession
import slamdata.Predef

class KafkaConsumerBuilder[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr](
    config: Config,
    tunnelSession: Option[TunnelSession],
    decoder: Decoder)
    extends ConsumerBuilder[F] {

  def mkFullConsumer: Resource[F, Consumer[F]] = {
    val F: ConcurrentEffect[F] = ConcurrentEffect[F]
    Resource.liftF(F delay {
      val recordDecoder = decoder match {
        case Decoder.RawKey => KafkaConsumerBuilder.RawKey[F]
        case Decoder.RawValue => KafkaConsumerBuilder.RawValue[F]
      }

      val consumerSettings = ConsumerSettings[F, Array[Byte], Array[Byte]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(config.bootstrapServers.toList.mkString(","))
//        .withBlocker(KafkaConsumerBuilder.blocker)  // uncomment if the default, single-threaded, blocker causes issues

      val proxyConsumerSettings =
        if (config.tunnelConfig.isEmpty) consumerSettings
        else consumerSettings.withCreateConsumer(ProxyKafkaConsumer(tunnelSession.orNull, _))

      FullConsumer(proxyConsumerSettings, recordDecoder)
    })
  }
}

object KafkaConsumerBuilder {
  import TunnelConfig._
  import Auth._

  private lazy val blocker: Blocker = quasar.concurrent.Blocker.cached("kafka-datasource")

  def resource[F[_] : ConcurrentEffect : ContextShift : Timer : MonadResourceErr](
      config: Config,
      decoder: Decoder)
      : Resource[F, ConsumerBuilder[F]] = {
    val tunnelSessionResource = config.tunnelConfig match {
      case Some(tunnelConfig) => viaTunnel(config, tunnelConfig, blocker).map(Some(_))
      case None               => Resource.pure(None)
    }
    for (tunnelSession <- tunnelSessionResource) yield new KafkaConsumerBuilder(config, tunnelSession, decoder)
  }

  // Decoders

  def RawKey[F[_]]: RecordDecoder[F, Array[Byte], Array[Byte]] =
    (record: CommittableConsumerRecord[F, Array[Byte], Array[Byte]]) =>
      Stream.chunk(Chunk.bytes(Option(record.record.key).getOrElse(Array.empty)))

  def RawValue[F[_]]: RecordDecoder[F, Array[Byte], Array[Byte]] =
    (record: CommittableConsumerRecord[F, Array[Byte], Array[Byte]]) =>
      Stream.chunk(Chunk.bytes(Option(record.record.value).getOrElse(Array.empty)))

  // Tunnel

  // TODO: move to package level
  final case class TunnelSession(tunnels: List[((String, Int), Int)]) {
    def ports: List[Int] = tunnels.map(_._2)
    def resolve(host: String, port: Int): Int =
      tunnels.find((host, port) == _._1).map(_._2).getOrElse(throw new UnknownHostException(s"$host:$port"))
    def hasTunnel(port: Predef.Int): Boolean = tunnels.exists(port == _._2)
  }

  object Address {
    val DefaultKafkaPort = 9092

    def unapply(address: String): Option[(String, Int)] = {
      val hostAndPort = address.split(":", 2) match {
        case Array(host)       => (host, DefaultKafkaPort)
        case Array(host, "")   => (host, DefaultKafkaPort)
        case Array(host, port) => (host, port.toInt)
      }
      Some(hostAndPort)
    }
  }

  val SessionName: String = "default"

  def F[F[_]: ConcurrentEffect]: ConcurrentEffect[F] = ConcurrentEffect[F]

  def viaTunnel[F[_]: ConcurrentEffect: ContextShift](
    config: Config,
    tunnelConfig: TunnelConfig,
    blocker: Blocker)
  : Resource[F, TunnelSession] = {
      Resource(ContextShift[F].blockOn(blocker) {
        for {
          jsch <- mkJSch
          session <- mkSession(jsch, tunnelConfig)
          _ <- setUserInfo(session, toUserInfo(tunnelConfig))
          tunnelSession <- openTunnel(session, config.bootstrapServers)
        } yield (tunnelSession, ContextShift[F].blockOn(blocker)(closeTunnel(session, tunnelSession)))
      })
  }

  def mkJSch[F[_]: ConcurrentEffect]: F[JSch] = F.delay(new JSch())

  def mkSession[F[_] : ConcurrentEffect](jsch: JSch, cfg: TunnelConfig): F[Session] = cfg.auth match {
    case None       =>
      F.delay(jsch.getSession(cfg.user, cfg.host, cfg.port))
    case Some(cred) =>
      cred match {
        case Password(password)             =>
          for {
            s <- F.delay(jsch.getSession(cfg.user, cfg.host, cfg.port))
            _ <- F.delay(s.setPassword(password))
          } yield s
        case Identity(prv, maybePassphrase) =>
          for {
            _ <- F delay {
              val passphrase = maybePassphrase.map(_.getBytes(utf8Charset)).orNull
              jsch.addIdentity(SessionName, prv.getBytes(utf8Charset), null, passphrase)
            }
            s <- F.delay(jsch.getSession(cfg.user, cfg.host, cfg.port))
          } yield s
      }
  }

  def openTunnel[F[_]: ConcurrentEffect](session: Session, addresses: NonEmptyList[String]): F[TunnelSession] = for {
    _ <- F.delay(session.connect())
    tunnels <- F.delay(for (Address(host, port) <- addresses)
      yield ((host, port), session.setPortForwardingL(0, host, port)))
  } yield TunnelSession(tunnels.toList)

  def closeTunnel[F[_]: ConcurrentEffect](session: Session, tunnelSession: TunnelSession): F[Unit] = for {
    _ <- F.delay(for (port <- tunnelSession.ports) yield session.delPortForwardingL(port))
    _ <- F.delay(session.disconnect())
  } yield ()

  def setUserInfo[F[_]: ConcurrentEffect](s: Session, u: UserInfo): F[Unit] = F.delay {
    s.setUserInfo(u)
  }

  def toUserInfo(cfg: TunnelConfig): UserInfo = new UserInfo {
    override def getPassword: String = cfg.getPassword
    override def getPassphrase: String = cfg.getPassphrase
    override def promptYesNo(s: String): Boolean = true
    override def promptPassphrase(s: String): Boolean = true
    override def promptPassword(s: String): Boolean = true
    override def showMessage(s: String): Unit = ()
  }
}
