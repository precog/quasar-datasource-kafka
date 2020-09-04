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

package quasar.datasource.kafka.proxy;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quasar.datasource.kafka.TunnelSession;

public final class ProxyUtils {
  private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

  private ProxyUtils() {
  }

  public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls,
                                                                  String clientDnsLookupConfig,
                                                                  TunnelSession tunnelSession) {
    return parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig), tunnelSession);
  }

  public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls,
                                                                  ClientDnsLookup clientDnsLookup,
                                                                  TunnelSession tunnelSession) {
    List<InetSocketAddress> addresses = new ArrayList<>();
    for (String url : urls) {
      if (url != null && !url.isEmpty()) {
        try {
          String host = getHost(url);
          Integer port = getPort(url);
          if (host == null || port == null)
            throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

          if (tunnelSession != null) {
              port = tunnelSession.resolve(host, port);
              host = "localhost";
          }

          if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
            InetAddress[] inetAddresses = InetAddress.getAllByName(host);
            for (InetAddress inetAddress : inetAddresses) {
              String resolvedCanonicalName = inetAddress.getCanonicalHostName();
              InetSocketAddress address = new InetSocketAddress(resolvedCanonicalName, port);
              if (address.isUnresolved()) {
                log.warn("Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedCanonicalName, host);
              } else {
                addresses.add(address);
              }
            }
          } else {
            InetSocketAddress address = new InetSocketAddress(host, port);
            if (address.isUnresolved()) {
              log.warn("Couldn't resolve server {} from {} as DNS resolution failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
            } else {
              addresses.add(address);
            }
          }

        } catch (IllegalArgumentException e) {
          throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
        } catch (UnknownHostException e) {
          throw new ConfigException("Unknown host in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
        }
      }
    }
    if (addresses.isEmpty())
      throw new ConfigException("No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    return addresses;
  }

  static List<InetAddress> resolve(String host, ClientDnsLookup clientDnsLookup) throws UnknownHostException {
      InetAddress[] addresses = InetAddress.getAllByName(host);
      if (ClientDnsLookup.USE_ALL_DNS_IPS == clientDnsLookup) {
          return filterPreferredAddresses(addresses);
      } else {
          return Collections.singletonList(addresses[0]);
      }
  }

  static List<InetAddress> filterPreferredAddresses(InetAddress[] allAddresses) {
      List<InetAddress> preferredAddresses = new ArrayList<>();
      Class<? extends InetAddress> clazz = null;
      for (InetAddress address : allAddresses) {
          if (clazz == null) {
              clazz = address.getClass();
          }
          if (clazz.isInstance(address)) {
              preferredAddresses.add(address);
          }
      }
      return preferredAddresses;
  }
}
