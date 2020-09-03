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

import java.lang.reflect.Modifier
import java.util.concurrent.TimeUnit
import java.util.{Collections, Locale, Optional, List => JList, Map => JMap}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.consumer.internals.PartitionAssignorAdapter.getAssignorInstances
import org.apache.kafka.clients.consumer.internals.{ConsumerCoordinator, ConsumerInterceptors, ConsumerMetadata, ConsumerNetworkClient, Fetcher, FetcherMetricsRegistry, SubscriptionState}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ClientUtils, CommonClientConfigs, GroupRebalanceConfig, NetworkClient}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerInterceptor, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{JmxReporter, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}

import cats.effect.ConcurrentEffect
import fs2.kafka.KafkaByteConsumer
import quasar.datasource.kafka.KafkaConsumerBuilder.TunnelSession

import scala.Predef.classOf
import scala.collection.JavaConverters._
import scala.sys

object ProxyKafkaConsumer {
  val ConsumerClientIdSequence = new AtomicInteger(1)
  val JmxPrefix = "kafka.consumer"
  val ClientIdMetricTag = "client-id"

  def apply[F[_]: ConcurrentEffect](tunnelSession: TunnelSession, properties: Map[String, String]): F[KafkaByteConsumer] = {

    ConcurrentEffect[F].delay {
      val byteArrayDeserializer = new ByteArrayDeserializer
      new proxy.KafkaConsumer[Array[Byte], Array[Byte]](
        (properties: Map[String, AnyRef]).asJava,
        byteArrayDeserializer,
        byteArrayDeserializer,
        tunnelSession
      )
    }
  }

  def buildClientId(configuredClientId: String, rebalanceConfig: GroupRebalanceConfig): String = {
    if (!configuredClientId.isEmpty) configuredClientId
    else if (rebalanceConfig.groupId != null && !rebalanceConfig.groupId.isEmpty)
      s"consumer-${rebalanceConfig.groupId}-${
        rebalanceConfig.groupInstanceId.orElseGet(() => ConsumerClientIdSequence.getAndIncrement.toString)}"
    else s"consumer-${ConsumerClientIdSequence.getAndIncrement}"
  }

  def buildMetrics(config: ConsumerConfig, time: Time, clientId: String): Metrics = {
    val metricsTags: JMap[String, String] = Collections.singletonMap(ClientIdMetricTag, clientId)
    val metricConfig: MetricConfig = new MetricConfig()
      .samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG).intValue())
      .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG).longValue(), TimeUnit.MILLISECONDS)
      .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
      .tags(metricsTags)
    val reporters: JList[MetricsReporter] = config.getConfiguredInstances(
      ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
      classOf[MetricsReporter],
      Collections.singletonMap[String, AnyRef](ConsumerConfig.CLIENT_ID_CONFIG, clientId))
    reporters.add(new JmxReporter(JmxPrefix))
    new Metrics(metricConfig, reporters, time)
  }

  def configureClusterResourceListeners(
      keyDeserializer: Deserializer[Array[Byte]],
      valueDeserializer: Deserializer[Array[Byte]],
      candidateLists: JList[_]*): ClusterResourceListeners = {
    val clusterResourceListeners = new ClusterResourceListeners
    for (candidateList <- candidateLists) clusterResourceListeners.maybeAddAll(candidateList)
    clusterResourceListeners.maybeAdd(keyDeserializer)
    clusterResourceListeners.maybeAdd(valueDeserializer)
    clusterResourceListeners
  }

}
