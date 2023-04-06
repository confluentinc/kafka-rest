/*
 * Copyright 2021 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfig;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProducerMetricsTest {
  private static final String METRICS_SEARCH_STRING = "kafka.rest:type=produce-api-metrics,*";

  private ProducerMetrics metrics;

  @BeforeEach
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(RestConfig.METRICS_JMX_PREFIX_CONFIG, "kafka.rest");
    metrics = new ProducerMetrics(new KafkaRestConfig(properties), new MockTime());
  }

  @Test
  public void testAvgMetrics() throws Exception {
    String[] avgMetrics =
        new String[] {
          ProducerMetrics.REQUEST_SIZE_AVG_METRIC_NAME,
          ProducerMetrics.REQUEST_LATENCY_AVG_METRIC_NAME
        };

    IntStream.range(0, 10)
        .forEach(
            n -> {
              metrics.recordRequestSize(n);
              metrics.recordRequestLatency(n);
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : avgMetrics) {
      assertEquals(4.5, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testRateMetrics() throws Exception {
    String[] rateMetrics =
        new String[] {
          ProducerMetrics.RECORD_ERROR_RATE_METRIC_NAME,
          ProducerMetrics.REQUEST_RATE_METRIC_NAME,
          ProducerMetrics.RESPONSE_RATE_METRIC_NAME
        };

    IntStream.range(0, 90)
        .forEach(
            n -> {
              metrics.recordError();
              metrics.recordRequest();
              metrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    // rate() uses a 90 second window here so one per second is correct
    for (String metric : rateMetrics) {
      assertEquals(1.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testMaxMetrics() throws Exception {
    String[] maxMetrics = new String[] {ProducerMetrics.REQUEST_LATENCY_MAX_METRIC_NAME};

    IntStream.range(0, 10).forEach(metrics::recordRequestLatency);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testPercentileMetrics() throws Exception {
    String[] percentileMetrics =
        new String[] {
          ProducerMetrics.REQUEST_LATENCY_PCT_METRIC_PREFIX + "p95",
          ProducerMetrics.REQUEST_LATENCY_PCT_METRIC_PREFIX + "p99",
          ProducerMetrics.REQUEST_LATENCY_PCT_METRIC_PREFIX + "p999",
        };

    IntStream.range(0, 1000).forEach(metrics::recordRequestLatency);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : percentileMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testWindowedCountMetrics() throws Exception {
    String[] maxMetrics =
        new String[] {
          ProducerMetrics.REQUEST_COUNT_WINDOWED_METRIC_NAME,
          ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME,
          ProducerMetrics.RESPONSE_COUNT_WINDOWED_METRIC_NAME
        };

    IntStream.range(0, 10)
        .forEach(
            n -> {
              metrics.recordRequest();
              metrics.recordError();
              metrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(10.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }
}
