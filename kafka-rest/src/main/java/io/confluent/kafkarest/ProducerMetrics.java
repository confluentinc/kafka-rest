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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMetrics {
  public static final String JMX_PREFIX = "kafka.rest";

  private static final Logger log = LoggerFactory.getLogger(ProducerMetrics.class);

  private final Metrics metrics;
  private final ConcurrentMap<BeanCoordinate, ProduceMetricMBean> beansByCoordinate =
      new ConcurrentHashMap<>();

  public ProducerMetrics(Time time) {
    this.metrics = new MetricsBuilder(JMX_PREFIX).withTime(time).build();
    setupMetricBeans();
    log.info("Successfully registered kafka-rest produce metrics with JMX");
  }

  /**
   * Get or create a {@link ProduceMetricMBean} with the specified name and the given tags. Each
   * group is uniquely identified by the name and tags.
   *
   * @param groupName the name of the ReplicatorMetricGroup group; may not be null
   * @param tags pairs of tag name and values
   * @return the {@link ProduceMetricMBean} that can be used to create metrics; never null
   */
  public ProduceMetricMBean mbean(String groupName, Map<String, String> tags) {
    BeanCoordinate beanCoordinate = new BeanCoordinate(groupName, tags);
    beansByCoordinate.putIfAbsent(beanCoordinate, new ProduceMetricMBean(beanCoordinate));
    return beansByCoordinate.get(beanCoordinate);
  }

  /** Stop and unregister the metrics from any reporters. */
  public void stop() {
    log.info("Unregistering produce metrics with JMX");
    metrics.close();
  }

  /** Sets up a {@link ProduceMetricMBean} for each sensor */
  private void setupMetricBeans() {
    ProduceMetricMBean metricMBean =
        mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap());

    // Clear out any previous metrics and sensors that might have been present
    if (metricMBean != null) {
      metricMBean.close();
    }

    // request metrics
    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_RATE)
        .withDoc(ProducerMetricsRegistry.REQUEST_RATE_DOC)
        .withRate()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_SIZE_AVG)
        .withDoc(ProducerMetricsRegistry.REQUEST_SIZE_AVG_DOC)
        .withAvg()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_SIZE_TOTAL)
        .withDoc(ProducerMetricsRegistry.REQUEST_SIZE_TOTAL_DOC)
        .withCumulativeSum()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_TOTAL)
        .withDoc(ProducerMetricsRegistry.REQUEST_TOTAL_DOC)
        .withCumulativeCount()
        .build();

    // response metrics
    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RESPONSE_SEND_RATE)
        .withDoc(ProducerMetricsRegistry.RESPONSE_SEND_RATE_DOC)
        .withRate()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RESPONSE_SIZE_AVG)
        .withDoc(ProducerMetricsRegistry.RESPONSE_SIZE_AVG_DOC)
        .withAvg()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RESPONSE_SIZE_TOTAL)
        .withDoc(ProducerMetricsRegistry.RESPONSE_SIZE_TOTAL_DOC)
        .withCumulativeSum()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RESPONSE_TOTAL)
        .withDoc(ProducerMetricsRegistry.RESPONSE_TOTAL_DOC)
        .withCumulativeCount()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RECORD_ERROR_RATE)
        .withDoc(ProducerMetricsRegistry.RECORD_ERROR_RATE_DOC)
        .withRate()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.RECORD_ERROR_TOTAL)
        .withDoc(ProducerMetricsRegistry.RECORD_ERROR_TOTAL_DOC)
        .withCumulativeCount()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_LATENCY_MAX)
        .withDoc(ProducerMetricsRegistry.REQUEST_LATENCY_MAX_DOC)
        .withMax()
        .build();

    new SensorBuilder(metricMBean)
        .withName(ProducerMetricsRegistry.REQUEST_LATENCY_AVG)
        .withDoc(ProducerMetricsRegistry.REQUEST_LATENCY_AVG_DOC)
        .withAvg()
        .build();
  }

  private class BeanCoordinate {
    private final String beanName;
    private final Map<String, String> tags;

    public BeanCoordinate(String beanName, Map<String, String> tags) {
      this.beanName = beanName;
      this.tags = tags;
    }

    public String getBeanName() {
      return beanName;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BeanCoordinate that = (BeanCoordinate) o;
      return Objects.equals(beanName, that.beanName) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(beanName, tags);
    }
  }

  /**
   * A class representing a JMX MBean where each metric maps to an MBean attribute. Sensors should
   * be added via the {@code sensor} methods on this class, rather than directly through the {@link
   * Metrics} class, so that the sensor names are made to be unique (based on the MBean name) and so
   * the sensors are removed when this group is {@link #close() closed}.
   */
  public class ProduceMetricMBean implements AutoCloseable {
    private final BeanCoordinate beanCoordinate;

    /**
     * Create a produce metrics MBean.
     *
     * @param beanCoordinate the identifier of the bean; may not be null and must be valid
     */
    ProduceMetricMBean(BeanCoordinate beanCoordinate) {
      Objects.requireNonNull(beanCoordinate);
      this.beanCoordinate = beanCoordinate;
    }

    public void recordMetrics(String sensorName, double value) {
      Sensor sensor =
          metrics.getSensor(
              String.join(":", JMX_PREFIX, ProducerMetricsRegistry.GROUP_NAME, sensorName));
      if (sensor != null) {
        sensor.record(value);
      }
    }

    /**
     * The {@link Metrics} that this MBean belongs to. Do not use this to add {@link Sensor
     * Sensors}, since they will not be removed when this group is{@link #close() closed}. Metrics
     * can be added directly, as long as the metric names are obtained from this group via the
     * {@link #metricName(MetricNameTemplate)} method.
     *
     * @return the metrics; never null
     */
    public Metrics metrics() {
      return metrics;
    }

    /**
     * Create the name of a metric that belongs to this group and has the group's tags.
     *
     * @param template the name template for the metric; may not be null
     * @return the metric name; never null
     * @throws IllegalArgumentException if the name is not valid
     */
    public MetricName metricName(MetricNameTemplate template) {
      return metrics.metricInstance(template, beanCoordinate.tags);
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors. This uses a default
     * recording level of INFO.
     *
     * @param name The sensor name
     * @return The sensor
     */
    synchronized Sensor sensor(String name) {
      Sensor sensor = metrics.sensor(name);
      return sensor;
    }

    /**
     * Add to this group an indicator metric that always returns the specified value.
     *
     * @param nameTemplate the name template for the metric; may not be null
     * @param value the value; may not be null
     * @throws IllegalArgumentException if the name is not valid
     */
    <T> void addImmutableValueMetric(MetricNameTemplate nameTemplate, final T value) {
      MetricName metricName = metricName(nameTemplate);
      if (metrics().metric(metricName) == null) {
        metrics().addMetric(metricName, (Gauge<T>) (config, now) -> value);
      }
    }

    /** Remove all sensors and metrics associated with this group. */
    @Override
    public synchronized void close() {
      for (MetricName metricName : new HashSet<>(metrics.metrics().keySet())) {
        if (beanCoordinate.beanName.equals(metricName.group())
            && beanCoordinate.tags.equals(metricName.tags())) {
          metrics.removeMetric(metricName);
        }
      }
    }
  }

  // I'm not going to lie this exists only to defeat checkstyle ;-)
  public static class SensorBuilder {

    private ProduceMetricMBean bean;
    private String name;
    private String doc;
    private MeasurableStat measurableStat;

    public SensorBuilder(ProduceMetricMBean bean) {
      this.bean = bean;
    }

    public SensorBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public SensorBuilder withDoc(String doc) {
      this.doc = doc;
      return this;
    }

    public SensorBuilder withAvg() {
      this.measurableStat = new Avg();
      return this;
    }

    public SensorBuilder withRate() {
      this.measurableStat = new Rate();
      return this;
    }

    public SensorBuilder withMax() {
      this.measurableStat = new Max();
      return this;
    }

    public SensorBuilder withCumulativeSum() {
      this.measurableStat = new CumulativeSum();
      return this;
    }

    public SensorBuilder withCumulativeCount() {
      this.measurableStat = new CumulativeCount();
      return this;
    }

    public Sensor build() {
      Sensor sensor =
          bean.sensor(String.join(":", JMX_PREFIX, ProducerMetricsRegistry.GROUP_NAME, name));
      MetricName metricName =
          bean.metricName(
              new MetricNameTemplate(
                  name, ProducerMetricsRegistry.GROUP_NAME, doc, Collections.emptySet()));
      sensor.add(metricName, measurableStat);
      return sensor;
    }
  }

  public static class MetricsBuilder {

    private MetricsContext metricsContext;
    private JmxReporter reporter;
    // 1 sample per second
    private int numSamples = 10;
    private long sampleWindowMs = 10000;
    private Time time = Time.SYSTEM;

    private Sensor.RecordingLevel level = Sensor.RecordingLevel.INFO;

    public MetricsBuilder(String prefix) {
      metricsContext = new KafkaMetricsContext(prefix);
      reporter = new JmxReporter();
      reporter.contextChange(metricsContext);
    }

    public MetricsBuilder withNumSamples(int numSamples) {
      this.numSamples = numSamples;
      return this;
    }

    public MetricsBuilder withSampleWindowMs(long sampleWindowMs) {
      this.sampleWindowMs = sampleWindowMs;
      return this;
    }

    public MetricsBuilder withLevel(Sensor.RecordingLevel level) {
      this.level = level;
      return this;
    }

    public MetricsBuilder withTime(Time time) {
      this.time = time;
      return this;
    }

    public Metrics build() {
      List<MetricsReporter> reporters = Collections.singletonList(reporter);
      MetricConfig metricConfig =
          new MetricConfig()
              .samples(numSamples)
              .timeWindow(sampleWindowMs, TimeUnit.MILLISECONDS)
              .recordLevel(level);
      return new Metrics(metricConfig, reporters, time, metricsContext);
    }
  }
}
