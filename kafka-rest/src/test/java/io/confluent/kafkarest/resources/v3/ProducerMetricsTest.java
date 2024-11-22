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

import static io.confluent.rest.RestConfig.METRICS_JMX_PREFIX_CONFIG;
import static java.util.Collections.singletonList;
import static java.util.function.LongUnaryOperator.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProducerMetricsTest {

  private static final String METRICS_SEARCH_STRING = "kafka.rest:type=produce-api-metrics,*";

  private ProducerMetrics producerMetrics;

  @BeforeEach
  public void setUp()
      throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException {
    Properties properties = new Properties();
    properties.setProperty(METRICS_JMX_PREFIX_CONFIG, "kafka.rest");

    KafkaRestConfig config = new KafkaRestConfig(properties);

    JmxReporter reporter = new JmxReporter();
    reporter.contextChange(config.getMetricsContext());
    // Metrics comes from rest-utils so mocking up here with the same config
    Metrics metrics =
        new Metrics(
            new MetricConfig()
                .samples(config.getInt(RestConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(
                    config.getLong(RestConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                    TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.INFO),
            singletonList(reporter),
            Time.SYSTEM,
            config.getMetricsContext());

    config.setMetrics(metrics);

    producerMetrics = new ProducerMetrics(config, ImmutableMap.of("tag", "value"));
  }

  @Test
  public void testAvgMetrics() throws Exception {
    String[] avgMetrics = new String[] {ProducerMetrics.REQUEST_LATENCY_AVG_METRIC_NAME};

    LongStream.range(0L, 10L).forEach(producerMetrics::recordRequestLatency);

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

    IntStream.range(0, 30)
        .forEach(
            n -> {
              producerMetrics.recordError();
              producerMetrics.recordRateLimited();
              producerMetrics.recordRequest();
              producerMetrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    // rate() uses the 30 second window we defined when creating the Metrics here so one per second
    // is correct
    for (String metric : rateMetrics) {
      assertThat(
          (Double) mBeanServer.getAttribute(beanNames.iterator().next(), metric),
          closeTo(1.0, 0.01));
    }
  }

  @Test
  public void testMaxMetrics() throws Exception {
    String[] maxMetrics = new String[] {ProducerMetrics.REQUEST_LATENCY_MAX_METRIC_NAME};

    LongStream.range(0L, 10L).forEach(producerMetrics::recordRequestLatency);

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

    LongStream.range(0L, 1000L).forEach(producerMetrics::recordRequestLatency);

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
              producerMetrics.recordRequest();
              producerMetrics.recordError();
              producerMetrics.recordRateLimited();
              producerMetrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(10.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testMeterBasedMetrics() throws Exception {
    LongStream.iterate(1L, identity()).limit(30).forEach(producerMetrics::recordRequestSize);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    assertEquals(30.0, mBeanServer.getAttribute(beanNames.iterator().next(), "request-byte-total"));
    // Time window is 30000ms, so this is one request per second across the window.
    assertThat(
        (Double) mBeanServer.getAttribute(beanNames.iterator().next(), "request-byte-rate"),
        closeTo(1.0, 0.01));
  }

  @Test
  public void testTenantTag() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());
    String tenantId = beanNames.stream().iterator().next().getKeyPropertyList().get("tag");
    assertEquals("value", tenantId);
  }
}
