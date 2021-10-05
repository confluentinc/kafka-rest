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

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMetrics {

  private static final Logger log = LoggerFactory.getLogger(ProducerMetrics.class);

  private final String fullyQualifiedRequestSensor;
  private final String fullyQualifiedRequestSizeSensor;
  private final String fullyQualifiedResponseSensor;
  private final String fullyQualifiedRecordErrorSensor;
  private final String fullyQualifiedRequestLatencySensor;

  private final String jmxPrefix;
  private final Metrics metrics;
  private final ConcurrentMap<BeanCoordinate, ProduceMetricMBean> beansByCoordinate =
      new ConcurrentHashMap<>();

  public ProducerMetrics(KafkaRestConfig config, Time time) {
    this.metrics = new MetricsBuilder(config.getMetricsContext()).withTime(time).build();
    this.jmxPrefix = config.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG);
    String sensorNameTemplate = jmxPrefix + ":" + ProducerMetricsRegistry.GROUP_NAME + ":%s";
    this.fullyQualifiedRecordErrorSensor =
        String.format(sensorNameTemplate, ProducerMetricsRegistry.RECORD_ERROR_SENSOR);
    this.fullyQualifiedRequestSensor =
        String.format(sensorNameTemplate, ProducerMetricsRegistry.REQUEST_SENSOR);
    this.fullyQualifiedRequestLatencySensor =
        String.format(sensorNameTemplate, ProducerMetricsRegistry.REQUEST_LATENCY_SENSOR);
    this.fullyQualifiedRequestSizeSensor =
        String.format(sensorNameTemplate, ProducerMetricsRegistry.REQUEST_SIZE_SENSOR);
    this.fullyQualifiedResponseSensor =
        String.format(sensorNameTemplate, ProducerMetricsRegistry.RESPONSE_SENSOR);
    setupMetricBeans();
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

  /** Sets up a {@link ProduceMetricMBean} for each sensor */
  private void setupMetricBeans() {
    ProduceMetricMBean metricMBean =
        mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap());

    // Clear out any previous metrics and sensors that might have been present
    if (metricMBean != null) {
      log.warn("closing pre-existing mBean:" + metricMBean);
      metricMBean.close();
    }

    // request metrics
    new SensorBuilder(metricMBean, jmxPrefix, ProducerMetricsRegistry.REQUEST_SENSOR)
        .addRate(ProducerMetricsRegistry.REQUEST_RATE, ProducerMetricsRegistry.REQUEST_RATE_DOC)
        .addWindowedCount(
            ProducerMetricsRegistry.REQUEST_COUNT_WINDOWED,
            ProducerMetricsRegistry.REQUEST_COUNT_WINDOWED_DOC)
        .build();

    new SensorBuilder(metricMBean, jmxPrefix, ProducerMetricsRegistry.REQUEST_SIZE_SENSOR)
        .addAvg(
            ProducerMetricsRegistry.REQUEST_SIZE_AVG, ProducerMetricsRegistry.REQUEST_SIZE_AVG_DOC)
        .build();

    // response metrics
    new SensorBuilder(metricMBean, jmxPrefix, ProducerMetricsRegistry.RESPONSE_SENSOR)
        .addRate(
            ProducerMetricsRegistry.RESPONSE_SEND_RATE,
            ProducerMetricsRegistry.RESPONSE_SEND_RATE_DOC)
        .addWindowedCount(
            ProducerMetricsRegistry.RESPONSE_COUNT_WINDOWED,
            ProducerMetricsRegistry.RESPONSE_COUNT_WINDOWED_DOC)
        .build();

    new SensorBuilder(metricMBean, jmxPrefix, ProducerMetricsRegistry.RECORD_ERROR_SENSOR)
        .addRate(
            ProducerMetricsRegistry.RECORD_ERROR_RATE,
            ProducerMetricsRegistry.RECORD_ERROR_RATE_DOC)
        .addWindowedCount(
            ProducerMetricsRegistry.ERROR_COUNT_WINDOWED,
            ProducerMetricsRegistry.ERROR_COUNT_WINDOWED_DOC)
        .build();

    new SensorBuilder(metricMBean, jmxPrefix, ProducerMetricsRegistry.REQUEST_LATENCY_SENSOR)
        .addMax(
            ProducerMetricsRegistry.REQUEST_LATENCY_MAX,
            ProducerMetricsRegistry.REQUEST_LATENCY_MAX_DOC)
        .addAvg(
            ProducerMetricsRegistry.REQUEST_LATENCY_AVG,
            ProducerMetricsRegistry.REQUEST_LATENCY_AVG_DOC)
        .addPercentiles(
            ProducerMetricsRegistry.REQUEST_LATENCY_PCT,
            ImmutableMap.of(
                "p95", 0.95,
                "p99", 0.99,
                "p999", 0.999),
            ProducerMetricsRegistry.REQUEST_LATENCY_PCT_DOC)
        .build();
    log.info("Successfully registered kafka-rest produce metrics with JMX");
  }

  private class BeanCoordinate {
    private final String beanName;
    private final Map<String, String> tags;

    public BeanCoordinate(String beanName, Map<String, String> tags) {
      this.beanName = Objects.requireNonNull(beanName);
      this.tags = ImmutableMap.copyOf(tags);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BeanCoordinate)) {
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
      this.beanCoordinate = Objects.requireNonNull(beanCoordinate);
    }

    public void recordResponse() {
      recordMetric(fullyQualifiedResponseSensor, 1.0);
    }

    public void recordRequestLatency(long value) {
      recordMetric(fullyQualifiedRequestLatencySensor, value);
    }

    public void recordError() {
      recordMetric(fullyQualifiedRecordErrorSensor, 1.0);
    }

    public void recordRequest() {
      recordMetric(fullyQualifiedRequestSensor, 1.0);
    }

    public void recordRequestSize(double value) {
      recordMetric(fullyQualifiedRequestSizeSensor, value);
    }

    private void recordMetric(String sensorName, double value) {
      Sensor sensor = metrics.getSensor(sensorName);
      if (sensor != null) {
        sensor.record(value);
      }
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

    /** Remove all sensors and metrics associated with this group. */
    @Override
    public synchronized void close() {
      for (MetricName metricName : new HashSet<>(metrics.metrics().keySet())) {
        if (metricName.group().equals(beanCoordinate.beanName)
            && beanCoordinate.tags.equals(metricName.tags())) {
          metrics.removeMetric(metricName);
        }
      }
    }
  }

  // I'm not going to lie this exists only to defeat checkstyle ;-)
  private static class SensorBuilder {

    private final ProduceMetricMBean bean;
    private final Sensor sensor;

    public SensorBuilder(ProduceMetricMBean bean, String jmxPrefix, String name) {
      this.bean = bean;
      this.sensor =
          bean.sensor(String.join(":", jmxPrefix, ProducerMetricsRegistry.GROUP_NAME, name));
    }

    public SensorBuilder addAvg(String name, String doc) {
      MetricName metricName = getMetricName(name, doc);
      sensor.add(metricName, MeasuredStatSupplier.avg());
      return this;
    }

    public SensorBuilder addRate(String name, String doc) {
      MetricName metricName = getMetricName(name, doc);
      sensor.add(metricName, MeasuredStatSupplier.rate());
      return this;
    }

    public SensorBuilder addMax(String name, String doc) {
      MetricName metricName = getMetricName(name, doc);
      sensor.add(metricName, MeasuredStatSupplier.max());
      return this;
    }

    public SensorBuilder addWindowedCount(String name, String doc) {
      MetricName metricName = getMetricName(name, doc);
      sensor.add(metricName, MeasuredStatSupplier.windowedCount());
      return this;
    }

    public SensorBuilder addPercentiles(
        String name, Map<String, Double> suffixPercentiles, String doc) {
      Map<MetricName, Double> namePercentiles =
          suffixPercentiles.entrySet().stream()
              .map(
                  suffixPercentile ->
                      new AbstractMap.SimpleImmutableEntry<MetricName, Double>(
                          getMetricName(name + suffixPercentile.getKey(), doc),
                          suffixPercentile.getValue()))
              .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

      sensor.add(MeasuredStatSupplier.percentiles(namePercentiles));
      return this;
    }

    public Sensor build() {
      return sensor;
    }

    private MetricName getMetricName(String name, String doc) {
      MetricName metricName =
          bean.metricName(
              new MetricNameTemplate(
                  name, ProducerMetricsRegistry.GROUP_NAME, doc, Collections.emptySet()));
      return metricName;
    }
  }

  private static class MeasuredStatSupplier {

    public static MeasurableStat avg() {
      return new Avg();
    }

    public static MeasurableStat rate() {
      return new Rate();
    }

    public static MeasurableStat max() {
      return new Max();
    }

    public static MeasurableStat windowedCount() {
      return new WindowedCount();
    }

    public static CompoundStat percentiles(Map<MetricName, Double> percentiles) {
      return new Percentiles(
          30 * 1000 * 4,
          30 * 1000,
          Percentiles.BucketSizing.CONSTANT,
          percentiles.entrySet().stream()
              .map(
                  namePercentile ->
                      new Percentile(namePercentile.getKey(), namePercentile.getValue()))
              .toArray(Percentile[]::new));
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

    public MetricsBuilder(MetricsContext metricsContext) {
      this.metricsContext = metricsContext;
      reporter = new JmxReporter();
      reporter.contextChange(metricsContext);
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
