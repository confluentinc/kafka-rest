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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProducerMetricsTest {

  private static final String METRICS_SEARCH_STRING = "kafka.rest:type=produce-api-metrics,*";

  private ProducerMetrics producerMetrics;
  private KafkaRestConfig config;

  private final Map<String, String> tags = ImmutableMap.of("tag", "value");

  @BeforeEach
  public void setUp()
      throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException {
    Properties properties = new Properties();
    properties.setProperty(METRICS_JMX_PREFIX_CONFIG, "kafka.rest");

    config = new KafkaRestConfig(properties);

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

    producerMetrics = new ProducerMetrics(config, tags);
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

  @Test
  public void testMultipleSensors() throws Exception {

    Map<String, String> tags2 = ImmutableMap.of("tag", "value2", "otherTag", "otherValue2");
    ProducerMetrics producerMetrics2 = new ProducerMetrics(config, tags2);

    producerMetrics.recordRequest();
    producerMetrics2.recordRequest();
    producerMetrics2.recordRequest();

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(2, beanNames.size());
    Iterator<ObjectName> i = beanNames.stream().iterator();
    String tenantId = i.next().getKeyPropertyList().get("tag");

    Hashtable<String, String> object2 = i.next().getKeyPropertyList();
    String tenantId2 = object2.get("tag");
    String otherValue2 = object2.get("otherTag");

    assertEquals("value", tenantId);
    assertEquals("value2", tenantId2);
    assertEquals("otherValue2", otherValue2);

    assertEquals(
        1.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,tag=value"),
            "request-count-windowed"));
    assertEquals(
        2.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,otherTag=otherValue2,tag=value2"),
            "request-count-windowed"));

    producerMetrics.recordRequest();
    producerMetrics2.recordRequest();
    producerMetrics2.recordRequest();

    assertEquals(
        2.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,tag=value"),
            "request-count-windowed"));
    assertEquals(
        4.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,otherTag=otherValue2,tag=value2"),
            "request-count-windowed"));

    mBeanServer.unregisterMBean(new ObjectName("kafka.rest:type=produce-api-metrics,tag=value"));
    mBeanServer.unregisterMBean(
        new ObjectName("kafka.rest:type=produce-api-metrics," + "otherTag=otherValue2,tag=value2"));
  }

  @Test
  public void test_requestSensor_hasCorrectMetricObjectTypeSetup() {
    {
      MetricName name = producerMetrics.getMetricName("request-rate", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name = producerMetrics.getMetricName("request-count-windowed", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof WindowedCount);
    }
  }

  @Test
  public void test_requestSizeSensor_hasCorrectMetricObjectTypeSetup() {
    // RequestSizeSensor setups up a meter, which internally comprises of a rate & cumulative-sum.
    // So to check a meter is created, check for rate & cum-sum.
    {
      MetricName name = producerMetrics.getMetricName("request-byte-rate", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name = producerMetrics.getMetricName("request-byte-total", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof CumulativeSum);
    }
  }

  @Test
  public void test_responseSensor_hasCorrectMetricObjectTypeSetup() {
    {
      MetricName name = producerMetrics.getMetricName("response-rate", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name = producerMetrics.getMetricName("response-count-windowed", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof WindowedCount);
    }
  }

  @Test
  public void test_recordErrorSensor_hasCorrectMetricObjectTypeSetup() {
    {
      MetricName name = producerMetrics.getMetricName("record-error-rate", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name = producerMetrics.getMetricName("error-count-windowed", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof WindowedCount);
    }
  }

  @Test
  public void test_recordRateLimitedSensor_hasCorrectMetricObjectTypeSetup() {
    {
      MetricName name = producerMetrics.getMetricName("record-rate-limited-rate", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name =
          producerMetrics.getMetricName("record-rate-limited-count-windowed", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof WindowedCount);
    }
  }

  @Test
  public void test_requestLatencySensor_hasCorrectMetricObjectTypeSetup() {
    {
      MetricName name = producerMetrics.getMetricName("request-latency-max", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Max);
    }
    {
      MetricName name = producerMetrics.getMetricName("request-latency-avg", "", tags);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Avg);
    }
  }
}
