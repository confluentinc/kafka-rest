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
import org.junit.jupiter.api.AfterEach;
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

  // ProducerMetrics now registers one MBean per http_status_code bucket (and one base bean), so
  // each test creates multiple beans on the platform MBeanServer. Without explicit cleanup the
  // beans leak across tests and risk InstanceAlreadyExistsException on re-registration or flaky
  // queryNames() counts in tests that check bean cardinality.
  @AfterEach
  public void tearDown() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    for (ObjectName beanName :
        mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null)) {
      try {
        mBeanServer.unregisterMBean(beanName);
      } catch (InstanceNotFoundException ignored) {
        // already gone — fine
      }
    }
  }

  @Test
  public void testAvgMetrics() throws Exception {
    String[] avgMetrics = new String[] {ProducerMetrics.REQUEST_LATENCY_AVG_METRIC_NAME};

    LongStream.range(0L, 10L).forEach(producerMetrics::recordRequestLatency);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    // Latency metrics live on the base MBean (no http_status_code tag).
    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");

    for (String metric : avgMetrics) {
      assertEquals(4.5, mBeanServer.getAttribute(baseBean, metric));
    }
  }

  @Test
  public void testRateMetrics() throws Exception {
    IntStream.range(0, 30)
        .forEach(
            n -> {
              producerMetrics.recordError(500);
              producerMetrics.recordRateLimited();
              producerMetrics.recordRequest();
              producerMetrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    // request/response rate live on the base MBean (no http_status_code tag)
    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");
    assertThat(
        (Double) mBeanServer.getAttribute(baseBean, ProducerMetrics.REQUEST_RATE_METRIC_NAME),
        closeTo(1.0, 0.01));
    assertThat(
        (Double) mBeanServer.getAttribute(baseBean, ProducerMetrics.RESPONSE_RATE_METRIC_NAME),
        closeTo(1.0, 0.01));

    // error rate lives on the per-status-code MBean — we recorded 500s so only the 5xx bean gets
    // increments; the 4xx and unknown beans should report ~0.
    ObjectName error5xxBean =
        new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=5xx,tag=value");
    assertThat(
        (Double)
            mBeanServer.getAttribute(error5xxBean, ProducerMetrics.RECORD_ERROR_RATE_METRIC_NAME),
        closeTo(1.0, 0.01));

    // rate-limited rate lives on the 429 MBean
    ObjectName rateLimited429Bean =
        new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=429,tag=value");
    assertThat(
        (Double)
            mBeanServer.getAttribute(
                rateLimited429Bean, ProducerMetrics.RECORD_RATE_LIMITED_RATE_METRIC_NAME),
        closeTo(1.0, 0.01));
  }

  @Test
  public void testMaxMetrics() throws Exception {
    String[] maxMetrics = new String[] {ProducerMetrics.REQUEST_LATENCY_MAX_METRIC_NAME};

    LongStream.range(0L, 10L).forEach(producerMetrics::recordRequestLatency);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");

    for (String metric : maxMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(baseBean, metric));
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
    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");

    for (String metric : percentileMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(baseBean, metric));
    }
  }

  @Test
  public void testWindowedCountMetrics() throws Exception {
    IntStream.range(0, 10)
        .forEach(
            n -> {
              producerMetrics.recordRequest();
              producerMetrics.recordError(500);
              producerMetrics.recordRateLimited();
              producerMetrics.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");
    assertEquals(
        10.0,
        mBeanServer.getAttribute(baseBean, ProducerMetrics.REQUEST_COUNT_WINDOWED_METRIC_NAME));
    assertEquals(
        10.0,
        mBeanServer.getAttribute(baseBean, ProducerMetrics.RESPONSE_COUNT_WINDOWED_METRIC_NAME));

    ObjectName error5xxBean =
        new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=5xx,tag=value");
    assertEquals(
        10.0,
        mBeanServer.getAttribute(
            error5xxBean, ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME));

    // 4xx and unknown buckets exist but were not incremented in this test
    ObjectName error4xxBean =
        new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=4xx,tag=value");
    assertEquals(
        0.0,
        mBeanServer.getAttribute(
            error4xxBean, ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME));

    ObjectName rateLimited429Bean =
        new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=429,tag=value");
    assertEquals(
        10.0,
        mBeanServer.getAttribute(
            rateLimited429Bean, ProducerMetrics.RECORD_RATE_LIMITED_COUNT_WINDOWED_METRIC_NAME));
  }

  @Test
  public void testRecordErrorBucketing() throws Exception {
    // 4xx (other than 429) routes to the 4xx bucket; 5xx routes to 5xx; codes outside 400-599
    // (e.g. 0 = no response code) route to the "unknown" bucket.
    producerMetrics.recordError(400);
    producerMetrics.recordError(404);
    producerMetrics.recordError(429);
    producerMetrics.recordError(500);
    producerMetrics.recordError(503);
    producerMetrics.recordError(0);
    producerMetrics.recordError(200);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    // 400, 404, 429 → 4xx bucket (3 increments)
    assertEquals(
        3.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=4xx,tag=value"),
            ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME));
    // 500, 503 → 5xx bucket (2 increments)
    assertEquals(
        2.0,
        mBeanServer.getAttribute(
            new ObjectName("kafka.rest:type=produce-api-metrics,http_status_code=5xx,tag=value"),
            ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME));
    // 0, 200 → unknown bucket (2 increments)
    assertEquals(
        2.0,
        mBeanServer.getAttribute(
            new ObjectName(
                "kafka.rest:type=produce-api-metrics,http_status_code=unknown,tag=value"),
            ProducerMetrics.RECORD_ERROR_COUNT_WINDOWED_METRIC_NAME));
  }

  @Test
  public void testHttpStatusCodeBucketing() {
    assertEquals(ProducerMetrics.STATUS_CODE_BUCKET_4XX, ProducerMetrics.httpStatusCodeBucket(400));
    assertEquals(ProducerMetrics.STATUS_CODE_BUCKET_4XX, ProducerMetrics.httpStatusCodeBucket(429));
    assertEquals(ProducerMetrics.STATUS_CODE_BUCKET_4XX, ProducerMetrics.httpStatusCodeBucket(499));
    assertEquals(ProducerMetrics.STATUS_CODE_BUCKET_5XX, ProducerMetrics.httpStatusCodeBucket(500));
    assertEquals(ProducerMetrics.STATUS_CODE_BUCKET_5XX, ProducerMetrics.httpStatusCodeBucket(599));
    assertEquals(
        ProducerMetrics.STATUS_CODE_BUCKET_UNKNOWN, ProducerMetrics.httpStatusCodeBucket(200));
    assertEquals(
        ProducerMetrics.STATUS_CODE_BUCKET_UNKNOWN, ProducerMetrics.httpStatusCodeBucket(600));
    assertEquals(
        ProducerMetrics.STATUS_CODE_BUCKET_UNKNOWN, ProducerMetrics.httpStatusCodeBucket(0));
  }

  @Test
  public void testMeterBasedMetrics() throws Exception {
    LongStream.iterate(1L, identity()).limit(30).forEach(producerMetrics::recordRequestSize);

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName baseBean = new ObjectName("kafka.rest:type=produce-api-metrics,tag=value");

    assertEquals(30.0, mBeanServer.getAttribute(baseBean, "request-byte-total"));
    // Time window is 30000ms, so this is one request per second across the window.
    assertThat(
        (Double) mBeanServer.getAttribute(baseBean, "request-byte-rate"), closeTo(1.0, 0.01));
  }

  @Test
  public void testTenantTag() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    // After KNET-19593 the producer metrics produce one base MBean (request/response/latency)
    // plus per-status-code MBeans for error (4xx/5xx/unknown) and rate-limited (429). Every
    // bean must carry the tenant tag.
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(5, beanNames.size());
    for (ObjectName beanName : beanNames) {
      assertEquals("value", beanName.getKeyPropertyList().get("tag"));
    }
  }

  @Test
  public void testMultipleSensors() throws Exception {

    Map<String, String> tags2 = ImmutableMap.of("tag", "value2", "otherTag", "otherValue2");
    ProducerMetrics producerMetrics2 = new ProducerMetrics(config, tags2);

    producerMetrics.recordRequest();
    producerMetrics2.recordRequest();
    producerMetrics2.recordRequest();

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

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
    // Error metrics are registered once per http_status_code bucket (4xx, 5xx, unknown).
    for (String bucket :
        new String[] {
          ProducerMetrics.STATUS_CODE_BUCKET_4XX,
          ProducerMetrics.STATUS_CODE_BUCKET_5XX,
          ProducerMetrics.STATUS_CODE_BUCKET_UNKNOWN
        }) {
      Map<String, String> tagsWithStatus =
          ImmutableMap.<String, String>builder()
              .putAll(tags)
              .put(ProducerMetrics.HTTP_STATUS_CODE_TAG, bucket)
              .build();
      {
        MetricName name = producerMetrics.getMetricName("record-error-rate", "", tagsWithStatus);
        KafkaMetric metric = config.getMetrics().metric(name);
        assertTrue(metric.measurable() instanceof Rate);
      }
      {
        MetricName name = producerMetrics.getMetricName("error-count-windowed", "", tagsWithStatus);
        KafkaMetric metric = config.getMetrics().metric(name);
        assertTrue(metric.measurable() instanceof WindowedCount);
      }
    }
  }

  @Test
  public void test_recordRateLimitedSensor_hasCorrectMetricObjectTypeSetup() {
    Map<String, String> tagsWithStatus =
        ImmutableMap.<String, String>builder()
            .putAll(tags)
            .put(ProducerMetrics.HTTP_STATUS_CODE_TAG, ProducerMetrics.STATUS_CODE_BUCKET_429)
            .build();
    {
      MetricName name =
          producerMetrics.getMetricName("record-rate-limited-rate", "", tagsWithStatus);
      KafkaMetric metric = config.getMetrics().metric(name);
      assertTrue(metric.measurable() instanceof Rate);
    }
    {
      MetricName name =
          producerMetrics.getMetricName("record-rate-limited-count-windowed", "", tagsWithStatus);
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
