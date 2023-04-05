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

package io.confluent.kafkarest;

import io.confluent.rest.metrics.RestMetricsContext;
import java.util.Map;
import org.apache.kafka.common.utils.AppInfoParser;


public final class KafkaRestMetricsContext {
  /**
   * MetricsContext Label's for use by Confluent's TelemetryReporter
   */
  private final RestMetricsContext metricsContext;

  public static final String RESOURCE_LABEL_PREFIX = "resource.";
  public static final String RESOURCE_LABEL_TYPE = RESOURCE_LABEL_PREFIX + "type";
  public static final String RESOURCE_LABEL_VERSION = RESOURCE_LABEL_PREFIX + "version";
  public static final String RESOURCE_LABEL_COMMIT_ID = RESOURCE_LABEL_PREFIX + "commit.id";
  public static final String RESOURCE_LABEL_CLUSTER_ID = RESOURCE_LABEL_PREFIX + "cluster.id";

  public static final String KAFKA_REST_RESOURCE_TYPE = "kafka_rest";
  public static final String KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT = "cluster_id_unavailable";

  public KafkaRestMetricsContext(String namespace, Map<String, Object> config) {
    metricsContext = new RestMetricsContext(namespace, config);

    setResourceLabel(RESOURCE_LABEL_TYPE, KAFKA_REST_RESOURCE_TYPE);
    setResourceLabel(RESOURCE_LABEL_CLUSTER_ID,
            (String) config.getOrDefault(RESOURCE_LABEL_CLUSTER_ID,
                    KAFKA_REST_RESOURCE_CLUSTER_ID_DEFAULT));
    setResourceLabel(RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    setResourceLabel(RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
  }

  /**
   * Sets {@link RestMetricsContext} resource label if not previously set.
   */
  private void setResourceLabel(String resource, String value) {
    if (metricsContext.getLabel(resource) == null) {
      metricsContext.setLabel(resource, value);
    }
  }

  /**
   * Returns internal RestMetricsContext
   */
  public RestMetricsContext metricsContext() {
    return metricsContext;
  }
}
