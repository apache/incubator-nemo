/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.master;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.exception.UnsupportedMetricException;
import edu.snu.nemo.runtime.common.metric.*;

import java.io.*;
import java.util.*;

/**
 * MetricStore stores metric data which will be used by web visualize interface, logging, and so on.
 */
public final class MetricStore {
  private final Map<Class, Map<String, Object>> metricMap = new HashMap<>();
  // You can add more metrics by adding item to this metricList list.
  private final Map<String, Class> metricList = new HashMap<String, Class>() {
    {
      put("JobMetric", JobMetric.class);
      put("StageMetric", StageMetric.class);
      put("TaskMetric", TaskMetric.class);
    }
  };

  /**
   * MetricStore is a class that collects all kind of metric which can be used at logging and web visualization.
   * All metric classes should be JSON-serializable by {@link ObjectMapper}.
   */
  private MetricStore() { }

  /**
   * Getter for singleton instance.
   * @return MetricStore singleton object.
   */
  public static MetricStore getStore() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Lazy class holder for MetricStore class.
   */
  private static class InstanceHolder {
    private static final MetricStore INSTANCE = new MetricStore();
  }

  public <T extends Metric> Class<T> getMetricClassByName(final String className) {
    if (!metricList.keySet().contains(className)) {
      throw new NoSuchElementException();
    }

    return metricList.get(className);
  }

  /**
   * Store a metric object. Metric object should implement {@link Metric} interface.
   * This method will store a metric into a {@link Map}, which have metric's id as its key.
   * @param metric metric object.
   * @param <T> class of metric
   */
  public <T extends Metric> void putMetric(final T metric) {
    final Class metricClass = metric.getClass();
    if (!metricList.values().contains(metricClass)) {
      throw new UnsupportedMetricException(new Throwable("Unsupported metric"));
    }

    metricMap.computeIfAbsent(metricClass, k -> new HashMap<>()).putIfAbsent(metric.getId(), metric);
  }

  /**
   * Fetch metric by its metric class instance and its id.
   * @param metricClass class instance of metric.
   * @param id metric id, which can be fetched by getId() method.
   * @param <T> class of metric
   * @return a metric object.
   */
  public <T extends Metric> T getMetricWithId(final Class<T> metricClass, final String id) {
    final T metric = (T) metricMap.computeIfAbsent(metricClass, k -> new HashMap<>()).get(id);
    if (metric == null) {
      throw new NoSuchElementException("No metric found");
    }
    return metric;
  }

  /**
   * Fetch metric map by its metric class instance.
   * @param metricClass class instance of metric.
   * @param <T> class of metric
   * @return a metric object.
   */
  public <T extends Metric> Map<String, Object> getMetricMap(final Class<T> metricClass) {
    final Map<String, Object> metric = metricMap.computeIfAbsent(metricClass, k -> new HashMap<>());
    if (metric == null) {
      throw new NoSuchElementException("No metric found");
    }
    return metric;
  }

  /**
   * Same as getMetricWithId(), but if there is no such metric, it will try to create new metric object
   * using its constructor, which takes an id as a parameter.
   * @param metricClass class of metric.
   * @param id metric id, which can be fetched by getId() method.
   * @param <T> class of metric
   * @return a metric object. If there was no such metric, newly create one.
   */
  public <T extends Metric> T getOrCreateMetric(final Class<T> metricClass, final String id) {
    T metric =  (T) metricMap.computeIfAbsent(metricClass, k -> new HashMap<>()).get(id);
    if (metric == null) {
      try {
        metric = metricClass.getConstructor(new Class[]{String.class}).newInstance(id);
        putMetric(metric);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return metric;
  }

  /**
   * Dumps JSON-serialized string of specific metric.
   * @param metricClass class of metric.
   * @return dumped JSON string of all metric.
   * @throws IOException when failed to write file.
   */
  public <T extends Metric> String dumpMetricToJson(final Class<T> metricClass) throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonFactory jsonFactory = new JsonFactory();
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
    jsonGenerator.setCodec(objectMapper);
    jsonGenerator.useDefaultPrettyPrinter();

    jsonGenerator.writeStartArray();
    for (final Map.Entry<String, Object> idToMetricEntry : getMetricMap(metricClass).entrySet()) {
      final JsonNode jsonNode = objectMapper.valueToTree(idToMetricEntry.getValue());
      jsonGenerator.writeTree(jsonNode);
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.close();
    return stream.toString();
  }

  /**
   * Dumps JSON-serialized string of all stored metric.
   * @return dumped JSON string of all metric.
   * @throws IOException when failed to write file.
   */
  public String dumpAllMetricToJson() throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonFactory jsonFactory = new JsonFactory();
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
    jsonGenerator.setCodec(objectMapper);
    jsonGenerator.useDefaultPrettyPrinter();

    for (final Map.Entry<Class, Map<String, Object>> metricMapEntry : metricMap.entrySet()) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(metricMapEntry.getKey().getSimpleName());
      jsonGenerator.writeStartArray();
      for (final Map.Entry<String, Object> idToMetricEntry : metricMapEntry.getValue().entrySet()) {
        final JsonNode jsonNode = objectMapper.valueToTree(idToMetricEntry.getValue());
        jsonGenerator.writeTree(jsonNode);
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }

    jsonGenerator.close();
    return stream.toString();
  }

  public void dumpAllMetricToFile(final String filePath) {
    try {
      final String jsonDump = dumpAllMetricToJson();
      final BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));

      writer.write(jsonDump);
      writer.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
