package io.confluent.kafkarest;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.mock.MockTime;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.stream.IntStream;
import javax.management.*;
import org.junit.Test;

public class ProducerMetricsTest {

  public static final String METRICS_SEARCH_STRING = "kafka.rest:type=produce-api-metrics,*";

  @Test
  public void testCountMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] totalMetrics =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_TOTAL,
          ProducerMetricsRegistry.RECORD_ERROR_TOTAL,
          ProducerMetricsRegistry.REQUEST_TOTAL
        };

    for (String metric : totalMetrics) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(metric, 1.0));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : totalMetrics) {
      assertEquals(10.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testSumMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] sumMetrics =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_SIZE_TOTAL, ProducerMetricsRegistry.REQUEST_SIZE_TOTAL
        };

    for (String metric : sumMetrics) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(metric, 10.0));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : sumMetrics) {

      assertEquals(100.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testAvgMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] avgMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SIZE_AVG,
          ProducerMetricsRegistry.REQUEST_LATENCY_AVG,
          ProducerMetricsRegistry.RESPONSE_SIZE_AVG
        };

    for (String metric : avgMetrics) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(metric, n));
    }

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
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] rateMetrics =
        new String[] {
          ProducerMetricsRegistry.RECORD_ERROR_RATE,
          ProducerMetricsRegistry.REQUEST_RATE,
          ProducerMetricsRegistry.RESPONSE_SEND_RATE
        };

    for (String metric : rateMetrics) {
      IntStream.range(0, 90).forEach(n -> mbean.recordMetrics(metric, 1.0));
    }

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
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] maxMetrics = new String[] {ProducerMetricsRegistry.REQUEST_LATENCY_MAX};

    for (String metric : maxMetrics) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(metric, n));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }
}
