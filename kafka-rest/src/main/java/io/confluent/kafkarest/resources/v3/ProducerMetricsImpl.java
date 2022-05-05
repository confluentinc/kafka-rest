/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.KafkaRestConfig;
import org.apache.kafka.common.utils.Time;

public class ProducerMetricsImpl extends AbstractProducerMetrics {

  ProducerMetricsImpl(KafkaRestConfig config, Time time) {
    this.metrics = createMetrics(config.getMetricsContext(), time);
    this.jmxPrefix = config.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG);
    String sensorNamePrefix = jmxPrefix + ":" + GROUP_NAME + ":";
    this.recordErrorSensorName = sensorNamePrefix + RECORD_ERROR_SENSOR_NAME;
    this.requestSensorName = sensorNamePrefix + REQUEST_SENSOR_NAME;
    this.requestLatencySensorName = sensorNamePrefix + REQUEST_LATENCY_SENSOR_NAME;
    this.requestSizeSensorName = sensorNamePrefix + REQUEST_SIZE_SENSOR_NAME;
    this.responseSensorName = sensorNamePrefix + RESPONSE_SENSOR_NAME;
    setupSensors();
  }
}
