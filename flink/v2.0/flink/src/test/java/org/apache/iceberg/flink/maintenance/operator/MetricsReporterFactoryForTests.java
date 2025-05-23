/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class MetricsReporterFactoryForTests implements MetricReporterFactory {
  private static final TestMetricsReporter INSTANCE = new TestMetricsReporter();
  private static final Pattern TASK_METRIC_NAME =
      Pattern.compile(
          "\\.taskmanager\\.[^.]+\\.[^.]+\\.([^.]+)\\.\\d+\\."
              + TableMaintenanceMetrics.GROUP_KEY
              + "\\."
              + TableMaintenanceMetrics.TABLE_NAME_KEY
              + "\\.([^.]+)\\."
              + TableMaintenanceMetrics.TASK_NAME_KEY
              + "\\.([^.]+)\\."
              + TableMaintenanceMetrics.TASK_INDEX_KEY
              + "\\.([^.]+)\\.([^.]+)");

  private static final Pattern MAIN_METRIC_NAME =
      Pattern.compile(
          "\\.taskmanager\\.[^.]+\\.[^.]+\\.([^.]+)\\.\\d+\\."
              + TableMaintenanceMetrics.GROUP_KEY
              + "\\."
              + TableMaintenanceMetrics.TABLE_NAME_KEY
              + "\\.([^.]+)\\.([^.]+)");

  private static Map<String, Counter> counters = Maps.newConcurrentMap();
  private static Map<String, Gauge> gauges = Maps.newConcurrentMap();
  private static Set<String> monitoredMetricNames;

  public MetricsReporterFactoryForTests() {
    monitoredMetricNames =
        Arrays.stream(TableMaintenanceMetrics.class.getDeclaredFields())
            .map(
                f -> {
                  try {
                    return f.get(null).toString();
                  } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());
  }

  @Override
  public MetricReporter createMetricReporter(Properties properties) {
    return INSTANCE;
  }

  public static void reset() {
    counters = Maps.newConcurrentMap();
    gauges = Maps.newConcurrentMap();
  }

  public static Long counter(List<String> parts) {
    return counterValues().get(longName(parts));
  }

  public static Long gauge(List<String> parts) {
    return gaugeValues().get(longName(parts));
  }

  public static void assertGauges(Map<List<String>, Long> expected) {
    Map<String, Long> transformed =
        expected.entrySet().stream()
            .collect(Collectors.toMap(k -> longName(k.getKey()), Map.Entry::getValue));
    assertThat(filter(gaugeValues(), transformed)).isEqualTo(filter(transformed, transformed));
  }

  public static void assertCounters(Map<List<String>, Long> expected) {
    Map<String, Long> transformed =
        expected.entrySet().stream()
            .collect(Collectors.toMap(k -> longName(k.getKey()), Map.Entry::getValue));
    assertThat(filter(counterValues(), transformed)).isEqualTo(filter(transformed, transformed));
  }

  private static Map<String, Long> gaugeValues() {
    return gauges.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> longName(entry.getKey()), entry -> (Long) entry.getValue().getValue()));
  }

  private static Map<String, Long> counterValues() {
    return counters.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> longName(entry.getKey()), entry -> entry.getValue().getCount()));
  }

  private static Map<String, Long> filter(Map<String, Long> original, Map<String, Long> filter) {
    return original.entrySet().stream()
        .filter(
            entry -> {
              Long filterValue = filter.get(entry.getKey());
              return filterValue == null || filterValue != -1;
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String longName(String fullName) {
    Matcher mainMatcher = MAIN_METRIC_NAME.matcher(fullName);
    Matcher taskMatcher = TASK_METRIC_NAME.matcher(fullName);

    if (taskMatcher.matches()) {
      return taskMatcher.group(1)
          + "."
          + taskMatcher.group(2)
          + "."
          + taskMatcher.group(3)
          + "."
          + taskMatcher.group(4)
          + "."
          + taskMatcher.group(5);
    }

    if (mainMatcher.matches()) {
      return mainMatcher.group(1) + "." + mainMatcher.group(2) + "." + mainMatcher.group(3);
    }

    throw new RuntimeException(String.format("Can't parse simplified metrics name %s", fullName));
  }

  private static String longName(List<String> parts) {
    return parts.stream().map(s -> s.replaceAll("\\.", "_")).collect(Collectors.joining("."));
  }

  private static class TestMetricsReporter implements MetricReporter {
    @Override
    public void open(MetricConfig config) {
      // do nothing
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
      if (monitoredMetricNames.contains(metricName)) {
        if (metric instanceof Counter) {
          counters.put(group.getMetricIdentifier(metricName), (Counter) metric);
        }

        if (metric instanceof Gauge) {
          gauges.put(group.getMetricIdentifier(metricName), (Gauge) metric);
        }
      }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
      // do nothing
    }
  }
}
