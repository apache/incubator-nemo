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

package org.apache.nemo.runtime.common.metric;

import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Utility class for metrics.
 * TODO #372: This class should later be refactored into a separate metric package.
 */
public final class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class.getName());

  private static final CountDownLatch METADATA_LOADED = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_KEY_METADATA = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_METADATA = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_PATTERN_METADATA = new CountDownLatch(1);

  // BiMap of (1) INDEX and (2) the Execution Property class and the value type class.
  static final HashBiMap<Integer, Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>>>
    EP_KEY_METADATA = HashBiMap.create();
  // BiMap of (1) the Execution Property class INDEX and the value INDEX pair and (2) the Execution Property value.
  private static final HashBiMap<Pair<Integer, Integer>, ExecutionProperty<? extends Serializable>>
    EP_METADATA = HashBiMap.create();
  // BiMap of (1) INDEX and (2) the sub-IRDAG corresponding to the pattern.
  private static final HashBiMap<Integer, IRDAG> PATTERN_METADATA = HashBiMap.create();

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new MetricException("PostgreSQL Driver not found: " + e);
    }
    loadMetaData();
  }

  public static final String SQLITE_DB_NAME =
    "jdbc:sqlite:" + Util.fetchProjectRootPath() + "/optimization_db.sqlite3";
  public static final String POSTGRESQL_METADATA_DB_NAME =
    "jdbc:postgresql://nemo-optimization.cabbufr3evny.us-west-2.rds.amazonaws.com:5432/nemo_optimization";
  private static final String EP_METADATA_TABLE_NAME = "ep_meta";
  private static final String PATTERN_METADATA_TABLE_NAME = "pattern_meta";

  /**
   * Private constructor.
   */
  private MetricUtils() {
  }

  /**
   * Load the BiMaps (lightweight) Metadata from the DB.
   *
   * @return the loaded BiMaps, or initialized ones.
   */
  private static void loadMetaData() {
    try (Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_METADATA_DB_NAME,
      "postgres", "fake_password")) {
      try (Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        statement.executeUpdate(
          "CREATE TABLE IF NOT EXISTS " + EP_METADATA_TABLE_NAME
            + " (type TEXT NOT NULL, key INT NOT NULL UNIQUE, value BYTEA NOT NULL, "
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");

        statement.executeUpdate(
          "CREATE TABLE IF NOT EXISTS " + PATTERN_METADATA_TABLE_NAME
            + " (key INT NOT NULL UNIQUE, value BYTEA NOT NULL, "
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");

        try (ResultSet rs1 = statement.executeQuery(
          "SELECT * FROM " + EP_METADATA_TABLE_NAME + " WHERE type='EP_KEY_METADATA';")) {
          LOG.info("Metadata can be successfully loaded.");
          while (rs1.next()) {
            try {
              EP_KEY_METADATA.put(rs1.getInt("key"),
                SerializationUtils.deserialize(rs1.getBytes("value")));
            } catch (SerializationException e) {
              LOG.warn("Couldn't deserialize EP_KEY and continuing: {}", rs1.getInt("key"));
            }
          }
        }

        try (ResultSet rs2 = statement.executeQuery(
          "SELECT * FROM " + EP_METADATA_TABLE_NAME + " WHERE type='EP_METADATA';")) {
          while (rs2.next()) {
            final Integer l = rs2.getInt("key");
            try {
              EP_METADATA.put(Pair.of(l / 10000, 1 % 10000),
                SerializationUtils.deserialize(rs2.getBytes("value")));
            } catch (SerializationException e) {
              LOG.warn("Couldn't deserialize EP and continuing: {}:{}", l / 10000, l % 10000);
            }
          }
        }

        try (ResultSet rs3 = statement.executeQuery(
          "SELECT * FROM " + PATTERN_METADATA_TABLE_NAME + ";")) {
          while (rs3.next()) {
            try {
              final Integer l = rs3.getInt("key");
              PATTERN_METADATA.put(l, SerializationUtils.deserialize(rs3.getBytes("value")));
            } catch (SerializationException e) {
              LOG.warn("Couldn't deserialize PATTERN and continuing: {}", rs3.getInt("key"));
            }
          }
        }

        METADATA_LOADED.countDown();
        LOG.info("Metadata successfully loaded from DB.");
      } catch (Exception e) {
        LOG.warn("Loading metadata from DB failed: ", e);
      }
    } catch (Exception e) {
      LOG.warn("Loading metadata from DB failed : ", e);
    }
  }

  public static Boolean metaDataLoaded() {
    return METADATA_LOADED.getCount() == 0;
  }

  /**
   * Save the BiMaps to DB if changes are necessary (rarely executed).
   */
  private static void updateMetaData() {
    if (!metaDataLoaded()
      || (MUST_UPDATE_EP_METADATA.getCount() + MUST_UPDATE_EP_KEY_METADATA.getCount()
      + MUST_UPDATE_PATTERN_METADATA.getCount() == 3)) {
      // no need to update
      LOG.info("Not saving Metadata: metadata loaded: {}, Index-EP data: {}, Index-EP Key data: {}, Pattern data: {}",
        metaDataLoaded(), MUST_UPDATE_EP_METADATA.getCount() == 0, MUST_UPDATE_EP_KEY_METADATA.getCount() == 0,
        MUST_UPDATE_PATTERN_METADATA.getCount() == 0);
      return;
    }
    LOG.info("Saving Metadata..");

    try (Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_METADATA_DB_NAME,
      "postgres", "fake_password")) {
      try (Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        if (MUST_UPDATE_EP_KEY_METADATA.getCount() == 0) {
          EP_KEY_METADATA.forEach((l, r) -> {
            try {
              insertOrUpdateMetadata(c, "EP_KEY_METADATA", l, r);
            } catch (SQLException e) {
              LOG.warn("Saving of Metadata to DB failed: ", e);
            }
          });
          LOG.info("EP Key Metadata saved to DB.");
        }

        if (MUST_UPDATE_EP_METADATA.getCount() == 0) {
          EP_METADATA.forEach((l, r) -> {
            try {
              insertOrUpdateMetadata(c, "EP_METADATA", l.left() * 10000 + l.right(), r);
            } catch (SQLException e) {
              LOG.warn("Saving of Metadata to DB failed: ", e);
            }
          });
          LOG.info("EP Metadata saved to DB.");
        }

        if (MUST_UPDATE_PATTERN_METADATA.getCount() == 0) {
          PATTERN_METADATA.forEach((l, r) -> {
            try {
              insertOrUpdateMetadata(c, PATTERN_METADATA_TABLE_NAME, l, r);
            } catch (SQLException e) {
              LOG.warn("Saving of Metadata to DB failed: ", e);
            }
          });
          LOG.info("PATTERN Metadata saved to DB.");
        }
      }
    } catch (SQLException e) {
      LOG.warn("Saving of Metadata to DB failed: ", e);
    }
  }

  /**
   * Utility method to save key, value to the metadata table.
   *
   * @param c     the connection to the DB.
   * @param type  the key to write to the DB metadata table, or the DB name to write the data on.
   * @param key   the key to write to the DB metadata table (integer).
   * @param value the value to write to the DB metadata table (object).
   * @throws SQLException SQLException on the way.
   */
  private static void insertOrUpdateMetadata(final Connection c, final String type,
                                             final Integer key, final Serializable value) throws SQLException {
    if (type.equals(PATTERN_METADATA_TABLE_NAME)) {
      try (PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + PATTERN_METADATA_TABLE_NAME
        + " (key, value) VALUES (" + key + ", ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value;")) {
        pstmt.setBinaryStream(1, new ByteArrayInputStream(SerializationUtils.serialize(value)));
        pstmt.executeUpdate();
      }
    } else {
      try (PreparedStatement pstmt = c.prepareStatement(
        "INSERT INTO " + EP_METADATA_TABLE_NAME + " (type, key, value) "
          + "VALUES ('" + type + "', " + key + ", ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value;")) {
        pstmt.setBinaryStream(1, new ByteArrayInputStream(SerializationUtils.serialize(value)));
        pstmt.executeUpdate();
      }
    }
  }

  /**
   * Stringify execution properties of an IR DAG.
   *
   * @param irdag the IR DAG to observe.
   * @return stringified execution properties, grouped as patterns. Left is for vertices, right is for edges.
   */
  public static Pair<String, String> stringifyIRDAGProperties(final IRDAG irdag) {
    return stringifyIRDAGProperties(irdag, 0);  // pattern recording is default
  }

  /**
   * Stringify execution properties of an IR DAG.
   *
   * @param irdag IR DAG to observe.
   * @param mode 0: record metrics by patterns, 1: record metrics with vertex or edge IDs.
   * @return the pair of stringified execution properties. Left is for vertices, right is for edges.
   */
  public static Pair<String, String> stringifyIRDAGProperties(final IRDAG irdag, final Integer mode) {
    final StringBuilder vStringBuilder = new StringBuilder();
    final StringBuilder eStringBuilder = new StringBuilder();

    if (mode == 0) {  // Patterns
      LOG.info("Vertices list: {}", irdag.getTopologicalSort());
      for (final IRVertex v: irdag.getTopologicalSort()) {
        final List<IREdge> incomingEdges = irdag.getIncomingEdgesOf(v);

        final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
        builder.addVertex(v);
        incomingEdges.forEach(e ->
          builder.addVertex(e.getSrc()).connectVertices(e));
        final IRDAG subDAG = new IRDAG(builder.buildWithoutSourceSinkCheck());

        // Get the index from the DAG components.
        final Integer index;
        final Optional<IRDAG> metaIRDAG = PATTERN_METADATA.values().stream()
          .filter(ird -> ird.getVertices().size() == subDAG.getVertices().size())  // same number of vertices
          .filter(ird -> {  // same topological signature
            final Iterator<IRVertex> it1 = ird.getTopologicalSort().iterator();
            final Iterator<IRVertex> it2 = subDAG.getTopologicalSort().iterator();
            while (it1.hasNext() && it2.hasNext()) {
              final String str1 = it1.next().toString();
              final String str2 = it2.next().toString();
              if (!str1.equals(str2)) {
                return false;
              }
            }
            return true;
          })
          .findFirst();

        if (metaIRDAG.isPresent()) {
          index = PATTERN_METADATA.inverse().get(metaIRDAG.get());
        } else {
          index = PATTERN_METADATA.keySet().stream().mapToInt(i -> i).max().orElse(-1) + 1;
          LOG.info("Inserting new pattern for {}({}), at index {}", v, v.getId(), index);
          PATTERN_METADATA.putIfAbsent(index, subDAG);
          MUST_UPDATE_PATTERN_METADATA.countDown();
        }

        // Let's now add the execution properties of the given pattern.
        final AtomicInteger idx = new AtomicInteger(0);

        // The vertex itself
        v.getExecutionProperties().forEachProperties(ep ->
          epFormatter(vStringBuilder, 1, index * 100 + idx.get(), ep));
        // Main incoming edges & vertex
        idx.getAndIncrement();
        incomingEdges.stream()
          .sorted(Comparator.comparing(e -> e.getSrc().toString()))
          .forEachOrdered(e -> {
            e.getExecutionProperties().forEachProperties(ep ->
              epFormatter(eStringBuilder, 1, index * 100 + idx.get(), ep));
            idx.getAndIncrement();
            e.getSrc().getExecutionProperties().forEachProperties(ep ->
              epFormatter(vStringBuilder, 1, index * 100 + idx.get(), ep));
            idx.getAndIncrement();
          });
      }
    } else {  // By Vertex / Edge IDs.
      irdag.getVertices().forEach(v ->
        v.getExecutionProperties().forEachProperties(ep ->
          epFormatter(vStringBuilder, 2, v.getNumericId(), ep)));

      irdag.getVertices().forEach(v ->
        irdag.getIncomingEdgesOf(v).forEach(e ->
          e.getExecutionProperties().forEachProperties(ep ->
            epFormatter(eStringBuilder, 3, e.getNumericId(), ep))));
    }

    // Update the metric metadata if new execution property key / values have been discovered and updates are required.
    updateMetaData();
    return Pair.of(vStringBuilder.toString().trim(), eStringBuilder.toString().trim());
  }

  /**
   * Formatter for execution properties. It updates the metadata for the metrics if new EP key / values are discovered.
   *
   * @param builder   string builder to append the metrics to.
   * @param idx       index specifying whether it's a pattern, a vertex or an edge. This should be one digit.
   * @param numericId numeric ID of the vertex or the edge.
   * @param ep        the execution property.
   */
  private static void epFormatter(final StringBuilder builder, final int idx,
                                  final Integer numericId, final ExecutionProperty<?> ep) {
    // Formatted into 9 digits: 0:vertex/edge 1-5:ID 5-9:EP Index.
    builder.append(idx);
    if (idx == 1) {  // pattern.
      builder.append(String.format("%05d", numericId));  // Pattern ID (3digit) and then index (2digit) (ID + 1: vertex,
      // 2: main incoming edge, 3: main incoming vertex, 4,6,8...: side-input edge, 5,7,9...: side-input vertex)
    } else if (idx == 2 || idx == 3) {  // vertex or edge ID.
      builder.append(String.format("%04d", numericId));  // ID in 4 digits
    }
    final Integer epKeyIndex = getEpKeyIndex(ep);
    builder.append(String.format("%04d", epKeyIndex));

    // Format value to an index.
    builder.append(":");
    final Integer epIndex = valueToIndex(epKeyIndex, ep);
    builder.append(epIndex);
    builder.append(" ");
  }

  /**
   * Get the EP Key index from the metadata.
   *
   * @param ep the EP to retrieve the Key index of.
   * @return the Key index.
   */
  static Integer getEpKeyIndex(final ExecutionProperty<?> ep) {
    return EP_KEY_METADATA.inverse()
      .computeIfAbsent(Pair.of(ep.getClass(), getParameterType(ep.getClass(), ep.getValue().getClass())),
        epClassPair -> {
          final Integer idx = EP_KEY_METADATA.keySet().stream().mapToInt(i -> i).max().orElse(-1) + 1;
          LOG.info("New EP Key Index: {} for {}", idx, epClassPair.left().getSimpleName());
          // Update the metadata if new EP key has been discovered.
          MUST_UPDATE_EP_KEY_METADATA.countDown();
          return idx;
        });
  }

  /**
   * Recursive method for getting the parameter type of the execution property.
   * This can be used, for example, to get DecoderFactory, instead of BeamDecoderFactory.
   *
   * @param epClass    execution property class to observe.
   * @param valueClass the value class of the execution property.
   * @return the parameter type.
   */
  private static Class<? extends Serializable> getParameterType(final Class<? extends ExecutionProperty> epClass,
                                                                final Class<? extends Serializable> valueClass) {
    if (!getMethodFor(epClass, "of", valueClass.getSuperclass()).isPresent()
      || !(Serializable.class.isAssignableFrom(valueClass.getSuperclass()))) {
      final Class<? extends Serializable> candidate = Arrays.stream(valueClass.getInterfaces())
        .filter(vc -> Serializable.class.isAssignableFrom(vc) && getMethodFor(epClass, "of", vc).isPresent())
        .map(vc -> getParameterType(epClass, ((Class<? extends Serializable>) vc))).findFirst().orElse(null);
      return candidate == null ? valueClass : candidate;
    } else {
      return getParameterType(epClass, ((Class<? extends Serializable>) valueClass.getSuperclass()));
    }
  }

  /**
   * Utility method to getting an optional method called 'name' for the class.
   *
   * @param clazz      class to get the method of.
   * @param name       the name of the method.
   * @param valueTypes the value types of the method.
   * @return optional of the method. It returns Optional.empty() if the method could not be found.
   */
  public static Optional<Method> getMethodFor(final Class<? extends ExecutionProperty> clazz,
                                              final String name, final Class<?>... valueTypes) {
    try {
      final Method mthd = clazz.getMethod(name, valueTypes);
      return Optional.of(mthd);
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Inverse method of the #getEpKeyIndex method.
   *
   * @param index the index of the EP Key.
   * @return the class of the execution property (EP), as well as the type of the value of the EP.
   */
  private static Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>> getEpPairFromKeyIndex(
    final Integer index) {
    return EP_KEY_METADATA.get(index);
  }

  /**
   * Return the sub DAG from the index.
   *
   * @param index the index of the sub-DAG.
   * @return the sub DAG of the corresponding pattern ID.
   */
  public static IRDAG getSubDAGFromIndex(final Integer index) {
    return PATTERN_METADATA.get(index);
  }

  /**
   * Helper method to convert Execution Property value objects to an integer index.
   * It updates the metadata for the metrics if new EP values are discovered.
   *
   * @param epKeyIndex the index of the execution property key.
   * @param ep         the execution property containing the value.
   * @return the converted value index.
   */
  static Integer valueToIndex(final Integer epKeyIndex, final ExecutionProperty<?> ep) {
    final Object o = ep.getValue();

    if (o instanceof Enum) {
      return ((Enum) o).ordinal();
    } else if (o instanceof Integer) {
      return (int) o;
    } else if (o instanceof Boolean) {
      return ((Boolean) o) ? 1 : 0;
    } else {
      final ExecutionProperty<? extends Serializable> ep1;
      if (o instanceof EncoderFactory || o instanceof DecoderFactory) {
        ep1 = EP_METADATA.values().stream()
          .filter(ep2 -> ep2.getValue().toString().equals(o.toString()) || ep2.getValue().equals(o))
          .findFirst().orElse(null);
      } else {
        ep1 = EP_METADATA.values().stream()
          .filter(ep2 -> ep2.getValue().equals(o))
          .findFirst().orElse(null);
      }

      if (ep1 != null) {
        return EP_METADATA.inverse().get(ep1).right();
      } else {
        final Integer valueIndex = EP_METADATA.keySet().stream()
          .filter(pair -> pair.left().equals(epKeyIndex))
          .mapToInt(Pair::right).max().orElse(-1) + 1;
        // Update the metadata if new EP value has been discovered.
        EP_METADATA.put(Pair.of(epKeyIndex, valueIndex), ep);
        LOG.info("New EP Index: ({}, {}) for {}", epKeyIndex, valueIndex, ep);
        MUST_UPDATE_EP_METADATA.countDown();
        return valueIndex;
      }
    }
  }

  /**
   * Helper method to do the opposite of the #valueToIndex method.
   * It receives the split, and the direction of the tweak value (which show the target index value),
   * and returns the actual value which the execution property uses.
   *
   * @param split      the split value, from which to start from.
   * @param tweak      the tweak value, to which we should tweak the split value.
   * @param epKeyIndex the EP Key index to retrieve information from.
   * @return the project root path.
   */
  static Serializable indexToValue(final Double split, final Double tweak, final Integer epKeyIndex) {
    final Class<? extends Serializable> targetObjectClass = getEpPairFromKeyIndex(epKeyIndex).right();
    final boolean splitIsInteger = split.compareTo((double) split.intValue()) == 0;
    final Pair<Integer, Integer> splitIntVal = splitIsInteger
      ? Pair.of(split.intValue() - 1, split.intValue() + 1)
      : Pair.of(split.intValue(), split.intValue() + 1);

    if (targetObjectClass.isEnum()) {
      final int ordinal;
      if (split < 0) {
        ordinal = 0;
      } else {
        final int maxOrdinal = targetObjectClass.getFields().length - 1;
        final int left = splitIntVal.left() <= 0 ? 0 : splitIntVal.left();
        final int right = splitIntVal.right() >= maxOrdinal ? maxOrdinal : splitIntVal.right();
        ordinal = tweak < 0 ? left : right;
      }
      LOG.info("Translated: {} into ENUM with ordinal {}", split, ordinal);
      return targetObjectClass.getEnumConstants()[ordinal];
    } else if (targetObjectClass.isAssignableFrom(Integer.class)) {
      final Double val = split + tweak + 0.5;
      final Integer res = val.intValue();
      LOG.info("Translated: {} into INTEGER of {}", split, res);
      return res;
    } else if (targetObjectClass.isAssignableFrom(Boolean.class)) {
      final Boolean res;
      if (split < 0) {
        res = false;
      } else if (split > 1) {
        res = true;
      } else {
        final Boolean left = splitIntVal.left() >= 1;  // false by default, true if >= 1
        final Boolean right = splitIntVal.right() > 0;  // true by default, false if <= 0
        res = tweak < 0 ? left : right;
      }
      LOG.info("Translated: {} into BOOLEAN of {}", split, res);
      return res;
    } else {
      final Supplier<IntStream> valueCandidates = () -> EP_METADATA.keySet().stream()
        .filter(p -> p.left().equals(epKeyIndex))
        .mapToInt(Pair::right);
      final Integer left = valueCandidates.get()
        .filter(n -> n < split)
        .map(n -> -n).sorted().map(n -> -n)  // maximum among smaller values
        .findFirst().orElse(valueCandidates.get().min().getAsInt());
      final Integer right = valueCandidates.get()
        .filter(n -> n > split)
        .sorted()  // minimum among larger values
        .findFirst().orElse(valueCandidates.get().max().getAsInt());
      final Integer targetValue = tweak < 0 ? left : right;
      final Serializable res = EP_METADATA.get(Pair.of(epKeyIndex, targetValue)).getValue();
      LOG.info("Translated: {} into VALUE of {}", split, res);
      return res;
    }
  }

  /**
   * Receives the pair of execution property and value classes, and returns the optimized value of the EP.
   *
   * @param epKeyIndex the EP Key index to retrieve the new EP from.
   * @param split      the split point.
   * @param tweak      the direction in which to tweak the execution property value.
   * @return The execution property constructed from the key index and the split value.
   */
  public static ExecutionProperty<? extends Serializable> keyAndValueToEP(
    final Integer epKeyIndex,
    final Double split,
    final Double tweak) {

    final Serializable value = indexToValue(split, tweak, epKeyIndex);
    final Class<? extends ExecutionProperty> epClass = getEpPairFromKeyIndex(epKeyIndex).left();

    final ExecutionProperty<? extends Serializable> ep;
    try {
      final Method staticConstructor = getMethodFor(epClass, "of", getParameterType(epClass, value.getClass()))
        .orElseThrow(NoSuchMethodException::new);
      ep = (ExecutionProperty<? extends Serializable>) staticConstructor.invoke(null, value);
    } catch (final NoSuchMethodException e) {
      throw new MetricException("Class " + epClass.getName()
        + " does not have a static method exposing the constructor 'of' with value type " + value.getClass().getName()
        + ": " + e);
    } catch (final Exception e) {
      throw new MetricException(e);
    }
    return ep;
  }
}
