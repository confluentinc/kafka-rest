/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.response;

import io.confluent.kafkarest.config.ConfigModule.AdvertisedListenersConfig;
import io.confluent.kafkarest.config.ConfigModule.HostNameConfig;
import io.confluent.kafkarest.config.ConfigModule.ListenersConfig;
import io.confluent.kafkarest.config.ConfigModule.PortConfig;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

public final class UrlFactoryImpl implements UrlFactory {

  private static final char SEPARATOR = '/';

  private final String baseUrl;

  @Inject
  public UrlFactoryImpl(
      @HostNameConfig String hostNameConfig,
      @PortConfig Integer portConfig,
      @AdvertisedListenersConfig List<URI> advertisedListenersConfig,
      @ListenersConfig List<URI> listenersConfig,
      @Context UriInfo requestUriInfo) {
    baseUrl =
        computeBaseUrl(
            hostNameConfig, portConfig, advertisedListenersConfig, listenersConfig, requestUriInfo);
  }

  @Override
  public String create(String... segments) {
    UrlBuilder urlBuilder = newUrlBuilder();
    for (String component : segments) {
      String stripped = trimSeparator(component);
      if (!stripped.isEmpty()) {
        urlBuilder.appendPathSegment(stripped);
      }
    }
    return urlBuilder.build();
  }

  @Override
  public UrlBuilder newUrlBuilder() {
    return new UrlBuilder(baseUrl);
  }

  private static String computeBaseUrl(
      String hostNameConfig,
      Integer portConfig,
      List<URI> advertisedListenersConfig,
      List<URI> listenersConfig,
      UriInfo requestUriInfo) {
    String scheme = computeScheme(requestUriInfo);
    String authority =
        computeAuthority(
            hostNameConfig, portConfig, advertisedListenersConfig, listenersConfig, requestUriInfo);
    String basePath = computeBasePath(requestUriInfo);

    StringBuilder baseUrl = new StringBuilder(scheme).append("://").append(authority);
    if (!basePath.isEmpty()) {
      baseUrl.append(SEPARATOR).append(basePath);
    }
    return baseUrl.toString();
  }

  private static String computeScheme(UriInfo requestUriInfo) {
    // Use same scheme as the incoming request.
    return requestUriInfo.getAbsolutePath().getScheme();
  }

  private static String computeAuthority(
      String hostNameConfig,
      Integer portConfig,
      List<URI> advertisedListenersConfig,
      List<URI> listenersConfig,
      UriInfo requestUriInfo) {
    return Stream.of(
            computeAuthorityFromAdvertisedListeners(advertisedListenersConfig, requestUriInfo),
            computeAuthorityFromHostNameAndPort(
                hostNameConfig, portConfig, listenersConfig, requestUriInfo))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst()
        .orElse(requestUriInfo.getAbsolutePath().getAuthority());
  }

  private static Optional<String> computeAuthorityFromAdvertisedListeners(
      List<URI> advertisedListenersConfig, UriInfo requestUriInfo) {
    String requestScheme = requestUriInfo.getAbsolutePath().getScheme();
    for (URI listener : advertisedListenersConfig) {
      if (requestScheme.equals(listener.getScheme())) {
        return Optional.of(listener.getAuthority());
      }
    }
    return Optional.empty();
  }

  private static Optional<String> computeAuthorityFromHostNameAndPort(
      String hostNameConfig,
      Integer portConfig,
      List<URI> listenersConfig,
      UriInfo requestUriInfo) {
    String requestScheme = requestUriInfo.getAbsolutePath().getScheme();
    if (hostNameConfig == null || hostNameConfig.isEmpty()) {
      return Optional.empty();
    }

    int port = -1;
    if (requestUriInfo.getAbsolutePath().getPort() != -1) {
      port = portConfig;
      for (URI listener : listenersConfig) {
        if (requestScheme.equals(listener.getScheme())) {
          port = listener.getPort();
        }
      }
    }

    return Optional.of(String.format("%s%s", hostNameConfig, port != -1 ? ":" + port : ""));
  }

  private static String computeBasePath(UriInfo requestUriInfo) {
    // Use same base path as the incoming request.
    return trimSeparator(requestUriInfo.getBaseUri().getPath());
  }

  private static String trimSeparator(String component) {
    int beginning = 0;
    while (beginning < component.length() && component.charAt(beginning) == SEPARATOR) {
      beginning++;
    }
    int end = component.length() - 1;
    while (end >= 0 && component.charAt(end) == SEPARATOR) {
      end--;
    }
    return beginning <= end ? component.substring(beginning, end + 1) : "";
  }
}
