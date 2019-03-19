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

package org.apache.nemo.common;

import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.exception.DeprecationException;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.exception.UnsupportedMethodException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Utility class for metrics.
 */
public final class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class.getName());

  private static final CountDownLatch METADATA_LOADED = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_KEY_METADATA = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_METADATA = new CountDownLatch(1);

  private static final Pair<HashBiMap<Integer, Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>>>,
      HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>>> METADATA = loadMetaData();
  // BiMap of (1) INDEX and (2) the Execution Property class and the value type class.
  private static final HashBiMap<Integer, Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>>>
    EP_KEY_METADATA = METADATA.left();
  // BiMap of (1) the Execution Property class INDEX and the value INDEX pair and (2) the Execution Property.
  private static final HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>>
    EP_METADATA = METADATA.right();

  private static final int VERTEX = 1;
  private static final int EDGE = 2;

  public static final String SQLITE_DB_NAME =
    "jdbc:sqlite:" + Util.fetchProjectRootPath() + "/optimization_db.sqlite3";
  public static final String POSTGRESQL_METADATA_DB_NAME =
    "jdbc:postgresql://nemo-optimization.cabbufr3evny.us-west-2.rds.amazonaws.com:5432/nemo_optimization";
  private static final String METADATA_TABLE_NAME = "nemo_optimization_meta";

  /**
   * Private constructor.
   */
  private MetricUtils() {
  }

  /**
   * Load the BiMaps (lightweight) Metadata from the DB.
   * @return the loaded BiMaps, or initialized ones.
   */
  private static Pair<HashBiMap<Integer, Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>>>,
    HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>>> loadMetaData() {
    deregisterBeamDriver();
    try (final Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_METADATA_DB_NAME,
      "postgres", "fake_password")) {
      try (final Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        statement.executeUpdate(
          "CREATE TABLE IF NOT EXISTS " + METADATA_TABLE_NAME
          + " (key TEXT NOT NULL UNIQUE, data BYTEA NOT NULL);");

        final ResultSet rsl = statement.executeQuery(
          "SELECT * FROM " + METADATA_TABLE_NAME + " WHERE key='EP_KEY_METADATA';");
        LOG.info("Metadata can be successfully loaded.");
        if (rsl.next()) {
          final HashBiMap<Integer, Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>>>
            indexEpKeyBiMap = SerializationUtils.deserialize(rsl.getBytes("Data"));
          rsl.close();

          final ResultSet rsr = statement.executeQuery(
            "SELECT * FROM " + METADATA_TABLE_NAME + " WHERE key='EP_METADATA';");
          if (rsr.next()) {
            final HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>> indexEpBiMap =
              SerializationUtils.deserialize(rsr.getBytes("Data"));
            rsr.close();

            METADATA_LOADED.countDown();
            LOG.info("Metadata successfully loaded from DB.");
            return Pair.of(indexEpKeyBiMap, indexEpBiMap);
          } else {
            METADATA_LOADED.countDown();
            LOG.info("No initial metadata for EP.");
            return Pair.of(indexEpKeyBiMap, HashBiMap.create());
          }
        } else {
          METADATA_LOADED.countDown();
          LOG.info("No initial metadata.");
          return Pair.of(HashBiMap.create(), HashBiMap.create());
        }
      } catch (Exception e) {
        LOG.warn("Loading metadata from DB failed: ", e);
        return Pair.of(HashBiMap.create(), HashBiMap.create());
      }
    } catch (Exception e) {
      LOG.warn("Loading metadata from DB failed : ", e);
      return Pair.of(HashBiMap.create(), HashBiMap.create());
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
      || (MUST_UPDATE_EP_METADATA.getCount() + MUST_UPDATE_EP_KEY_METADATA.getCount() == 2)) {
      // no need to update
      LOG.info("Not saving Metadata: metadata loaded: {}, Index-EP data: {}, Index-EP Key data: {}",
        metaDataLoaded(), MUST_UPDATE_EP_METADATA.getCount() == 0, MUST_UPDATE_EP_KEY_METADATA.getCount() == 0);
      return;
    }
    LOG.info("Saving Metadata..");

    deregisterBeamDriver();
    try (final Connection c = DriverManager.getConnection(MetricUtils.POSTGRESQL_METADATA_DB_NAME,
      "postgres", "fake_password")) {
      try (final Statement statement = c.createStatement()) {
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        if (MUST_UPDATE_EP_KEY_METADATA.getCount() == 0) {
          insertOrUpdateMetadata(c, "EP_KEY_METADATA", EP_KEY_METADATA);
          LOG.info("EP Key Metadata saved to DB.");
        }

        if (MUST_UPDATE_EP_METADATA.getCount() == 0) {
          insertOrUpdateMetadata(c, "EP_METADATA", EP_METADATA);
          LOG.info("EP Metadata saved to DB.");
        }
      }
    } catch (SQLException e) {
      LOG.warn("Saving of Metadata to DB failed: ", e);
    }
  }

  /**
   * Utility method to save key, value to the metadata table.
   * @param c the connection to the DB.
   * @param key the key to write to the DB metadata table.
   * @param value the value to write to the DB metadata table.
   * @throws SQLException SQLException on the way.
   */
  public static void insertOrUpdateMetadata(final Connection c, final String key, final Serializable value)
    throws SQLException {
    try (final PreparedStatement pstmt = c.prepareStatement(
      "INSERT INTO " + METADATA_TABLE_NAME + " (key, data) "
        + "VALUES ('" + key + "', ?) ON CONFLICT (key) DO UPDATE SET data = excluded.data;")) {
      pstmt.setBinaryStream(1,
        new ByteArrayInputStream(SerializationUtils.serialize(value)));
      pstmt.executeUpdate();
    }
  }

  /**
   * Stringify execution properties of an IR DAG.
   * @param irdag IR DAG to observe.
   * @return the pair of stringified execution properties. Left is for vertices, right is for edges.
   */
  public static Pair<String, String> stringifyIRDAGProperties(final IRDAG irdag) {
    final StringBuilder vStringBuilder = new StringBuilder();
    final StringBuilder eStringBuilder = new StringBuilder();

    irdag.getVertices().forEach(v ->
      v.getExecutionProperties().forEachProperties(ep ->
        epFormatter(vStringBuilder, VERTEX, v.getNumericId(), ep)));

    irdag.getVertices().forEach(v ->
      irdag.getIncomingEdgesOf(v).forEach(e ->
        e.getExecutionProperties().forEachProperties(ep ->
          epFormatter(eStringBuilder, EDGE, e.getNumericId(), ep))));

    // Update the metric metadata if new execution property key / values have been discovered and updates are required.
    updateMetaData();
    return Pair.of(vStringBuilder.toString().trim(), eStringBuilder.toString().trim());
  }

  /**
   * Formatter for execution properties. It updates the metadata for the metrics if new EP key / values are discovered.
   * @param builder string builder to append the metrics to.
   * @param idx index specifying whether it's a vertex or an edge. This should be one digit.
   * @param numericId numeric ID of the vertex or the edge.
   * @param ep the execution property.
   */
  private static void epFormatter(final StringBuilder builder, final int idx,
                                  final Integer numericId, final ExecutionProperty<?> ep) {
    // Formatted into 9 digits: 0:vertex/edge 1-5:ID 5-9:EP Index.
    builder.append(idx);
    builder.append(String.format("04%d", numericId));
    final Integer epKeyIndex = getEpKeyIndex(ep);
    builder.append(String.format("04%d", epKeyIndex));

    // Format value to an index.
    builder.append(":");
    final Integer epIndex = valueToIndex(epKeyIndex, ep);
    builder.append(epIndex);
    builder.append(" ");
  }

  /**
   * Get the EP Key index from the metadata.
   * @param ep the EP to retrieve the Key index of.
   * @return the Key index.
   */
  static Integer getEpKeyIndex(final ExecutionProperty<?> ep) {
    return EP_KEY_METADATA.inverse()
      .computeIfAbsent(Pair.of(ep.getClass(), ep.getValueClass()), epClassPair -> {
        LOG.info("New EP Key Index: {} for {}", EP_KEY_METADATA.size() + 1, epClassPair.left().getSimpleName());
        // Update the metadata if new EP key has been discovered.
        MUST_UPDATE_EP_KEY_METADATA.countDown();
        return EP_KEY_METADATA.size() + 1;
      });
  }

  /**
   * Inverse method of the #getEpKeyIndex method.
   * @param index the index of the EP Key.
   * @return the class of the execution property (EP), as well as the type of the value of the EP.
   */
  static Pair<Class<? extends ExecutionProperty>, Class<? extends Serializable>> getEpPairFromKeyIndex(
    final Integer index) {
    return EP_KEY_METADATA.get(index);
  }

  /**
   * Helper method to convert Execution Property value objects to an integer index.
   * It updates the metadata for the metrics if new EP values are discovered.
   * @param epKeyIndex the index of the execution property key.
   * @param ep the execution property containing the value.
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
      final ExecutionProperty<?> ep1;
      if (o instanceof EncoderFactory || o instanceof DecoderFactory) {
        ep1 = EP_METADATA.values().stream()
          .filter(ep2 -> ep2.getValue().toString().equals(o.toString()))
          .findFirst().orElse(null);
      } else {
        ep1 = EP_METADATA.values().stream()
          .filter(ep2 -> ep2.getValue().equals(o))
          .findFirst().orElse(null);
      }

      if (ep1 != null) {
        return EP_METADATA.inverse().get(ep1).right();
      } else {
        final Integer valueIndex = Math.toIntExact(EP_METADATA.keySet().stream()
          .filter(pair -> pair.left().equals(epKeyIndex))
          .count()) + 1;
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
   * @param split the split value, from which to start from.
   * @param tweak the tweak value, to which we should tweak the split value.
   * @param epKeyIndex the EP Key index to retrieve information from.
   * @return the value object reflecting the split and the tweak values.
   */
  static Object indexToValue(final Double split, final Double tweak, final Integer epKeyIndex) {
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
        .filter(n -> n < split.intValue())
        .sorted()
        .findFirst().orElse(valueCandidates.get().min().getAsInt());
      final Integer right = valueCandidates.get()
        .filter(n -> n > split.intValue())
        .map(n -> -n).sorted().map(n -> -n)
        .findFirst().orElse(valueCandidates.get().max().getAsInt());
      final Integer targetValue = tweak < 0 ? left : right;
      return EP_METADATA.get(Pair.of(epKeyIndex, targetValue)).getValue();
    }
  }

  /**
   * Restore the formatted string into a pair of vertex/edge id and the execution property.
   * @param string the formatted string.
   * @return a pair of vertex/edge id and the execution property key index.
   */
  public static Pair<String, Integer> stringToIdAndEPKeyIndex(
    final String string) {
    if (string.length() != 9) {
      throw new DeprecationException("The metric data is deprecated or does not follow the designated format");
    }
    final Integer idx = Integer.parseInt(string.substring(0, 1));
    final Integer numericId = Integer.parseInt(string.substring(1, 5));
    final String id;
    if (idx == VERTEX) {
      id = Vertex.restoreId(numericId);
    } else if (idx == EDGE) {
      id = Edge.restoreId(numericId);
    } else {
      throw new UnsupportedMethodException("The index " + idx + " cannot be categorized into a vertex or an edge");
    }
    return Pair.of(id, Integer.parseInt(string.substring(5, 9)));
  }

  /**
   * Receives the pair of execution property and value classes, and returns the optimized value of the EP.
   * @param epKeyIndex the EP Key index to retrieve the new EP from.
   * @param split the split point.
   * @param tweak the direction in which to tweak the execution property value.
   * @return the tweaked execution property.
   */
  public static ExecutionProperty<? extends Serializable> pairAndValueToEP(
    final Integer epKeyIndex,
    final Double split,
    final Double tweak) {

    final Object value = indexToValue(split, tweak, epKeyIndex);
    final Class<? extends ExecutionProperty> epClass = getEpPairFromKeyIndex(epKeyIndex).left();

    final ExecutionProperty<? extends Serializable> ep;
    try {
      final Method staticConstructor = epClass.getMethod("of", value.getClass());
      ep =
        (ExecutionProperty<? extends Serializable>) staticConstructor.invoke(null, value);
    } catch (final NoSuchMethodException e) {
      throw new MetricException("Class " + epClass.getName()
        + " does not have a static method exposing the constructor 'of' with value type " + value.getClass().getName()
        + ": " + e);
    } catch (final Exception e) {
      throw new MetricException(e);
    }
    return ep;
  }

  /**
   * Types of environments to categorize them into.
   */
  public enum EnvironmentType {
    DEFAULT,
    TRANSIENT,
    LARGE_SHUFFLE,
    DISAGGREGATION,
    DATA_SKEW,
    STREAMING,
    SMALL_SIZE,
  }

  /**
   * Method to infiltrate keyword-containing string into the enum of Types above.
   * @param environmentType the input string.
   * @return the formatted string corresponding to each type.
   */
  public static String filterEnvironmentTypeString(final String environmentType) {
    if (environmentType.toLowerCase().contains("transient")) {
      return "_" + EnvironmentType.TRANSIENT.toString().toLowerCase();
    } else if (environmentType.toLowerCase().contains("large") && environmentType.toLowerCase().contains("shuffle")) {
      return "_" + EnvironmentType.LARGE_SHUFFLE.toString().toLowerCase();
    } else if (environmentType.toLowerCase().contains("disaggregat")) {
      return "_" + EnvironmentType.DISAGGREGATION.toString().toLowerCase();
    } else if (environmentType.toLowerCase().contains("stream")) {
      return "_" + EnvironmentType.STREAMING.toString().toLowerCase();
    } else if (environmentType.toLowerCase().contains("small")) {
      return "_" + EnvironmentType.SMALL_SIZE.toString().toLowerCase();
    } else if (environmentType.toLowerCase().contains("skew")) {
      return "_" + EnvironmentType.DATA_SKEW.toString().toLowerCase();
    } else {
      return "";  // Default
    }
  }

  /**
   * De-register Beam JDBC driver, which produces inconsistent results.
   * This is solved in newer Beam versions. Delete this when updating the Beam version.
   */
  public static void deregisterBeamDriver() {
    final String beamDriver = "org.apache.beam.sdk.extensions.sql.impl.JdbcDriver";
    final Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      final Driver d = drivers.nextElement();
      if (d.getClass().getName().equals(beamDriver)) {
        try {
          DriverManager.deregisterDriver(d);
        } catch (SQLException e) {
          throw new MetricException(e);
        }
        break;
      }
    }
  }
}
