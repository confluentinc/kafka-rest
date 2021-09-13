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

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SENSOR,
          ProducerMetricsRegistry.RECORD_ERROR_SENSOR,
          ProducerMetricsRegistry.RESPONSE_SENSOR,
        };

    String[] totalMetrics =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_TOTAL,
          ProducerMetricsRegistry.RECORD_ERROR_TOTAL,
          ProducerMetricsRegistry.REQUEST_TOTAL
        };

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, 1.0));
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

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_SIZE_SENSOR, ProducerMetricsRegistry.REQUEST_SIZE_SENSOR,
        };

    String[] sumMetrics =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_SIZE_TOTAL, ProducerMetricsRegistry.REQUEST_SIZE_TOTAL
        };

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, 10.0));
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

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.RESPONSE_SIZE_SENSOR,
          ProducerMetricsRegistry.REQUEST_SIZE_SENSOR,
          ProducerMetricsRegistry.REQUEST_LATENCY_SENSOR
        };

    String[] avgMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SIZE_AVG,
          ProducerMetricsRegistry.REQUEST_LATENCY_AVG,
          ProducerMetricsRegistry.RESPONSE_SIZE_AVG
        };

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, n));
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

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.RECORD_ERROR_SENSOR,
          ProducerMetricsRegistry.REQUEST_SENSOR,
          ProducerMetricsRegistry.RESPONSE_SENSOR
        };

    String[] rateMetrics =
        new String[] {
          ProducerMetricsRegistry.RECORD_ERROR_RATE,
          ProducerMetricsRegistry.REQUEST_RATE,
          ProducerMetricsRegistry.RESPONSE_SEND_RATE
        };

    for (String sensor : sensors) {
      IntStream.range(0, 90).forEach(n -> mbean.recordMetrics(sensor, 1.0));
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

    String[] sensors = new String[] {ProducerMetricsRegistry.REQUEST_LATENCY_SENSOR};

    String[] maxMetrics = new String[] {ProducerMetricsRegistry.REQUEST_LATENCY_MAX};

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, n));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(9.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testWindowedCountMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, InterruptedException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SENSOR,
          ProducerMetricsRegistry.RESPONSE_SENSOR,
          ProducerMetricsRegistry.RECORD_ERROR_SENSOR
        };

    String[] maxMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_TOTAL_WINDOWED,
          ProducerMetricsRegistry.RECORD_ERROR_TOTAL_WINDOWED,
          ProducerMetricsRegistry.RESPONSE_TOTAL_WINDOWED
        };

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, n));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(10.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }

  @Test
  public void testWindowedSumMetrics()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
          AttributeNotFoundException, MBeanException, InterruptedException, IntrospectionException {
    Time mockTime = new MockTime();
    ProducerMetrics metrics = new ProducerMetrics(mockTime);

    ProducerMetrics.ProduceMetricMBean mbean = metrics.mbean("kafka.rest", Collections.emptyMap());

    String[] sensors =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SIZE_SENSOR, ProducerMetricsRegistry.RESPONSE_SIZE_SENSOR
        };

    String[] maxMetrics =
        new String[] {
          ProducerMetricsRegistry.REQUEST_SIZE_TOTAL_WINDOWED,
          ProducerMetricsRegistry.RESPONSE_SIZE_TOTAL_WINDOWED
        };

    for (String sensor : sensors) {
      IntStream.range(0, 10).forEach(n -> mbean.recordMetrics(sensor, n));
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beanNames = mBeanServer.queryNames(new ObjectName(METRICS_SEARCH_STRING), null);
    assertEquals(1, beanNames.size());

    for (String metric : maxMetrics) {
      assertEquals(45.0, mBeanServer.getAttribute(beanNames.iterator().next(), metric));
    }
  }
}
