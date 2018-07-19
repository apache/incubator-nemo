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
 * All metric classes should be JSON-serializable by {@link ObjectMapper}.
 */
public final class MetricStore {
  private final Map<Class, Map<String, Object>> metricMap = new HashMap<>();
  // You can add more metrics by adding item to this metricList list.
  private final Map<String, Class> metricList = new HashMap<>();
  /**
   * Private constructor.
   */
  private MetricStore() {
    metricList.put("JobMetric", JobMetric.class);
    metricList.put("StageMetric", StageMetric.class);
    metricList.put("TaskMetric", TaskMetric.class);
  }

  /**
   * Getter for singleton instance.
   * @return MetricStore object.
   */
  public static MetricStore getStore() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Lazy class object holder for MetricStore class.
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

  private void generatePreprocessedJsonFromMetricEntry(final Map.Entry<String, Object> idToMetricEntry,
                                                       final JsonGenerator jsonGenerator,
                                                       final ObjectMapper objectMapper) throws IOException {
    final JsonNode jsonNode = objectMapper.valueToTree(idToMetricEntry.getValue());
    jsonGenerator.writeFieldName(idToMetricEntry.getKey());
    jsonGenerator.writeStartObject();
    jsonGenerator.writeFieldName("id");
    jsonGenerator.writeString(idToMetricEntry.getKey());
    jsonGenerator.writeFieldName("data");
    jsonGenerator.writeTree(jsonNode);
    jsonGenerator.writeEndObject();
  }

  /**
   * Dumps JSON-serialized string of specific metric.
   * @param metricClass class of metric.
   * @return dumped JSON string of all metric.
   * @throws IOException when failed to write json.
   */
  public <T extends Metric> String dumpMetricToJson(final Class<T> metricClass) throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonFactory jsonFactory = new JsonFactory();
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
    jsonGenerator.setCodec(objectMapper);

    jsonGenerator.writeStartObject();
    jsonGenerator.writeFieldName(metricClass.getSimpleName());
    jsonGenerator.writeStartObject();
    for (final Map.Entry<String, Object> idToMetricEntry : getMetricMap(metricClass).entrySet()) {
      generatePreprocessedJsonFromMetricEntry(idToMetricEntry, jsonGenerator, objectMapper);
    }
    jsonGenerator.writeEndObject();
    jsonGenerator.writeEndObject();

    jsonGenerator.close();
    return stream.toString();
  }

  /**
   * Dumps JSON-serialized string of all stored metric.
   * @return dumped JSON string of all metric.
   * @throws IOException when failed to write file.
   */
  public synchronized String dumpAllMetricToJson() throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonFactory jsonFactory = new JsonFactory();
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
    jsonGenerator.setCodec(objectMapper);

    jsonGenerator.writeStartObject();
    for (final Map.Entry<Class, Map<String, Object>> metricMapEntry : metricMap.entrySet()) {
      jsonGenerator.writeFieldName(metricMapEntry.getKey().getSimpleName());
      jsonGenerator.writeStartObject();
      for (final Map.Entry<String, Object> idToMetricEntry : metricMapEntry.getValue().entrySet()) {
        generatePreprocessedJsonFromMetricEntry(idToMetricEntry, jsonGenerator, objectMapper);
      }
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndObject();

    jsonGenerator.close();
    return stream.toString();
  }

  /**
   * Same as dumpAllMetricToJson(), but this will save it to the file.
   * @param filePath path to dump JSON.
   */
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


  /**
   * Send changed metric data to {@link MetricBroadcaster}, which will broadcast it to
   * all active WebSocket sessions. This method should be called manually if you want to
   * send changed metric data to the frontend client. Also this method is synchronized.
   * @param metricClass class of the metric.
   * @param id id of the metric.
   */
  public synchronized <T extends Metric> void triggerBroadcast(final Class<T> metricClass, final String id) {
    final MetricBroadcaster metricBroadcaster = MetricBroadcaster.getInstance();
    final ObjectMapper objectMapper = new ObjectMapper();
    final T metric = getMetricWithId(metricClass, id);
    final JsonFactory jsonFactory = new JsonFactory();
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator;
    try {
      jsonGenerator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);

      jsonGenerator.setCodec(objectMapper);

      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("metricType");
      jsonGenerator.writeString(metricClass.getSimpleName());

      jsonGenerator.writeFieldName("data");
      jsonGenerator.writeObject(metric);
      jsonGenerator.writeEndObject();

      jsonGenerator.close();

      metricBroadcaster.broadcast(stream.toString());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
