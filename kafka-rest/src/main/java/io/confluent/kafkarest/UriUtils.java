/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest;

import io.confluent.kafkarest.response.UrlFactoryImpl;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.core.UriInfo;
import org.apache.kafka.common.config.ConfigException;

public class UriUtils {

  public static String absoluteUri(KafkaRestConfig config, UriInfo uriInfo, String... components) {
    List<URI> advertisedListeners;
    List<URI> listeners;
    try {
      advertisedListeners =
          config.getList(KafkaRestConfig.ADVERTISED_LISTENERS_CONFIG).stream()
              .map(URI::create)
              .collect(Collectors.toList());
      listeners =
          config.getList(KafkaRestConfig.LISTENERS_CONFIG).stream()
              .map(URI::create)
              .collect(Collectors.toList());
    } catch (IllegalArgumentException e) {
      throw new ConfigException(e.getMessage());
    }

    return new UrlFactoryImpl(
            config.getString(KafkaRestConfig.HOST_NAME_CONFIG),
            config.getInt(KafkaRestConfig.PORT_CONFIG),
            advertisedListeners,
            listeners,
            uriInfo)
        .create(components);
  }
}
