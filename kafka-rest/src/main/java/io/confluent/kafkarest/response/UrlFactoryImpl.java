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

import com.google.common.collect.Iterables;
import io.confluent.kafkarest.config.ConfigModule.AdvertisedListenersConfig;
import io.confluent.kafkarest.config.ConfigModule.HostNameConfig;
import io.confluent.kafkarest.config.ConfigModule.ListenersConfig;
import io.confluent.kafkarest.config.ConfigModule.PortConfig;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

final class UrlFactoryImpl implements UrlFactory {

  private static final char SEPARATOR = '/';

  private final String baseUrl;

  @Inject
  UrlFactoryImpl(
      @HostNameConfig String hostNameConfig,
      @PortConfig Integer portConfig,
      @AdvertisedListenersConfig List<String> advertisedListenersConfig,
      @ListenersConfig List<String> listenersConfig,
      @Context UriInfo requestUriInfo
  ) {
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
      List<String> advertisedListenersConfig,
      List<String> listenersConfig,
      UriInfo requestUriInfo
  ) {
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
      List<String> advertisedListenersConfig,
      List<String> listenersConfig,
      UriInfo requestUriInfo) {
    // Preferences are, in order:
    // 1. hostNameConfig:portConfig
    // 2. listener.authority, where listener in advertisedListenersConfig and
    //                        listener.scheme = request.scheme
    // 3. listener.authority, where listener in listenersConfig and
    //                        listener.scheme = request.scheme
    // 4. request.authority
    if (!hostNameConfig.isEmpty()) {
      return String.format("%s:%s", hostNameConfig, portConfig);
    }
    String requestScheme = requestUriInfo.getAbsolutePath().getScheme();
    for (String listener : Iterables.concat(advertisedListenersConfig, listenersConfig)) {
      int protocolSeparator = listener.indexOf("://");
      String listenerScheme = listener.substring(0, protocolSeparator);
      if (requestScheme.equals(listenerScheme)) {
        return listener.substring(protocolSeparator + 3);
      }
    }
    return requestUriInfo.getAbsolutePath().getAuthority();
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
