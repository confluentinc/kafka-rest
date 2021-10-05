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

package io.confluent.kafkarest;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.metrics.RestMetricsContext;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.stream.IntStream;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ProducerMetricsTest {

  public static final String METRICS_SEARCH_STRING = "kafka.rest:type=produce-api-metrics,*";
  public static final RestMetricsContext REST_METRICS_CONTEXT =
      new RestMetricsContext("kafka.rest", ImmutableMap.of());
  public static final String NAMESPACE = "kafka.rest";

  @Rule public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock private KafkaRestConfig kafkaRestConfig;

  @Before
  public void setUpMocks() {
    reset(kafkaRestConfig);
    expect(kafkaRestConfig.getMetricsContext()).andReturn(REST_METRICS_CONTEXT);
    expect(kafkaRestConfig.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG))
        .andReturn(NAMESPACE);
    replay(kafkaRestConfig);
  }

  @Test
  public void testAvgMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException {
    Time mockTime = new MockTime();

    ProducerMetrics metrics = new ProducerMetrics(kafkaRestConfig, mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean(NAMESPACE, Collections.emptyMap());

    String[] avgMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SIZE_AVG, ProducerMetricsRegistry.REQUEST_LATENCY_AVG
        };

    IntStream.range(0, 10)
        .forEach(
            n -> {
              mbean.recordRequestSize(n);
              mbean.recordRequestLatency(n);
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : avgMetrics) {
      assertEquals(4.5, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testRateMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, InterruptedException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(kafkaRestConfig, mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] rateMetrics =
        new String[] {
          ProducerMetricsRegistry.RECORD_ERROR_RATE,
          ProducerMetricsRegistry.REQUEST_RATE,
          ProducerMetricsRegistry.RESPONSE_SEND_RATE
        };

    IntStream.range(0, 90)
        .forEach(
            n -> {
              mbean.recordError();
              mbean.recordRequest();
              mbean.recordResponse();
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
  public void testMaxMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, InterruptedException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(kafkaRestConfig, mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] maxMetrics = new String[] {ProducerMetricsRegistry.REQUEST_LATENCY_MAX};

    IntStream.range(0, 10).forEach(n -> mbean.recordRequestLatency(n));

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testPercentileMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, InterruptedException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(kafkaRestConfig, mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] percentileMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_LATENCY_PCT + "p95",
          ProducerMetricsRegistry.REQUEST_LATENCY_PCT + "p99",
          ProducerMetricsRegistry.REQUEST_LATENCY_PCT + "p999",
        };

    IntStream.range(0, 1000).forEach(n -> mbean.recordRequestLatency(n));

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : percentileMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testWindowedCountMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(kafkaRestConfig, mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] maxMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_COUNT_WINDOWED,
          ProducerMetricsRegistry.ERROR_COUNT_WINDOWED,
          ProducerMetricsRegistry.RESPONSE_COUNT_WINDOWED
        };

    IntStream.range(0, 10)
        .forEach(
            n -> {
              mbean.recordRequest();
              mbean.recordError();
              mbean.recordResponse();
            });

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(10.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }
}
