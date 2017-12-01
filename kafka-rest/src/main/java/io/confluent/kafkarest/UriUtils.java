/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

public class UriUtils {

  public static UriBuilder absoluteUriBuilder(KafkaRestConfig config, UriInfo uriInfo) {
    String hostname = config.getString(KafkaRestConfig.HOST_NAME_CONFIG);
    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
    if (hostname.length() > 0) {
      builder.host(hostname);
      // Resetting the hostname removes the scheme and port for some reason, so they may need to
      // be reset.
      URI origAbsoluteUri = uriInfo.getAbsolutePath();
      builder.scheme(origAbsoluteUri.getScheme());
      // Only reset the port if it was set in the original URI
      if (origAbsoluteUri.getPort() != -1) {
        builder.port(config.getInt(KafkaRestConfig.PORT_CONFIG));
      }
    }
    return builder;
  }
}
