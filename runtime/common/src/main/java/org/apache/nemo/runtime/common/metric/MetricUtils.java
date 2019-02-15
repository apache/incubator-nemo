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
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * Utility class for metrics.
 */
public final class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class.getName());

  private static final CountDownLatch METADATA_LOADED = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_KEY_METADATA = new CountDownLatch(1);
  private static final CountDownLatch MUST_UPDATE_EP_METADATA = new CountDownLatch(1);

  private static final Pair<HashBiMap<Integer, Class<? extends ExecutionProperty>>,
    HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>>> METADATA = loadMetaData();
  // BiMap of (1) INDEX and (2) the Execution Property class
  private static final HashBiMap<Integer, Class<? extends ExecutionProperty>>
    EP_KEY_METADATA = METADATA.left();
  // BiMap of (1) the Execution Property class INDEX and the value INDEX pair and (2) the Execution Property.
  private static final HashBiMap<Pair<Integer, Integer>, ExecutionProperty<?>>
    EP_METADATA = METADATA.right();

  private static final int VERTEX = 1;
  private static final int EDGE = 2;

  public static final String SQLITE_DB_NAME =
    "jdbc:sqlite:" + MetricUtils.fetchProjectRootPath() + "/optimization_db.sqlite3";
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
  private static Pair<HashBiMap<Integer, Class<? extends ExecutionProperty>>,
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
        LOG.info("Metadata can be loaded.");
        if (rsl.next()) {
          final HashBiMap<Integer, Class<? extends ExecutionProperty>> indexEpKeyBiMap =
            SerializationUtils.deserialize(rsl.getBytes("Data"));
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
          try (final PreparedStatement pstmt = c.prepareStatement(
            "INSERT INTO " + METADATA_TABLE_NAME + " (key, data) "
              + "VALUES ('EP_KEY_METADATA', ?) ON CONFLICT (key) DO UPDATE SET data = excluded.data;")) {
            pstmt.setBinaryStream(1,
              new ByteArrayInputStream(SerializationUtils.serialize(EP_KEY_METADATA)));
            pstmt.executeUpdate();
            LOG.info("EP Key Metadata saved to DB.");
          }
        }

        if (MUST_UPDATE_EP_METADATA.getCount() == 0) {
          try (final PreparedStatement pstmt =
                 c.prepareStatement("INSERT INTO " + METADATA_TABLE_NAME + "(key, data) "
                     + "VALUES ('EP_METADATA', ?) ON CONFLICT (key) DO UPDATE SET data = excluded.data;")) {
            pstmt.setBinaryStream(1,
              new ByteArrayInputStream(SerializationUtils.serialize(EP_METADATA)));
            pstmt.executeUpdate();
            LOG.info("EP Metadata saved to DB.");
          }
        }
      }
    } catch (SQLException e) {
      LOG.warn("Saving of Metadata to DB failed: ", e);
    }
  }

  /**
   * Stringify execution properties of an IR DAG.
   * @param irdag IR DAG to observe.
   * @return the pair of stringified execution properties. Left is for vertices, right is for edges.
   */
  static Pair<String, String> stringifyIRDAGProperties(final IRDAG irdag) {
    final StringBuilder vStringBuilder = new StringBuilder();
    final StringBuilder eStringBuilder = new StringBuilder();

    irdag.getVertices().forEach(v ->
      v.getExecutionProperties().forEachProperties(ep ->
        epFormatter(vStringBuilder, VERTEX, v.getNumericId(), ep)));

    irdag.getVertices().forEach(v ->
      irdag.getIncomingEdgesOf(v).forEach(e ->
        e.getExecutionProperties().forEachProperties(ep ->
          epFormatter(eStringBuilder, EDGE, e.getNumericId(), ep))));

//    Update the metric metadata if new execution property key / values have been discovered and updates are required.
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
    builder.append(idx);
    builder.append(numericId);
    builder.append("0");
    final Integer epKeyIndex = EP_KEY_METADATA.inverse().computeIfAbsent(ep.getClass(), epClass -> {
//      Update the metadata if new EP key has been discovered.
      LOG.info("New EP Key Index: {} for {}", EP_KEY_METADATA.size() + 1, epClass.getSimpleName());
      MUST_UPDATE_EP_KEY_METADATA.countDown();
      return EP_KEY_METADATA.size() + 1;
    });
    builder.append(epKeyIndex);

    builder.append(":");
    final Integer epIndex = valueToIndex(epKeyIndex, ep);
    builder.append(epIndex);
    builder.append(" ");
  }

  /**
   * Helper method to convert Execution Property value objects to an integer index.
   * It updates the metadata for the metrics if new EP values are discovered.
   * @param epKeyIndex the index of the execution property key.
   * @param ep the execution property containing the value.
   * @return the converted value index.
   */
  private static Integer valueToIndex(final Integer epKeyIndex, final ExecutionProperty<?> ep) {
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
//      Update the metadata if new EP value has been discovered.
        EP_METADATA.put(Pair.of(epKeyIndex, valueIndex), ep);
        LOG.info("New EP Index: ({}, {}) for {}", epKeyIndex, valueIndex, ep);
        MUST_UPDATE_EP_METADATA.countDown();
        return valueIndex;
      }
    }
  }

  /**
   * Finds the project root path by looking for the travis.yml file.
   * @return the project root path.
   */
  private static String fetchProjectRootPath() {
    return recursivelyFindLicense(Paths.get(System.getProperty("user.dir")));
  }

  /**
   * Helper method to recursively find the LICENSE file.
   * @param path the path to search for.
   * @return the path containing the LICENSE file.
   */
  private static String recursivelyFindLicense(final Path path) {
    try (final Stream stream = Files.find(path, 1, (p, attributes) -> p.endsWith("LICENSE"))) {
      if (stream.count() > 0) {
        return path.toAbsolutePath().toString();
      } else {
        return recursivelyFindLicense(path.getParent());
      }
    } catch (IOException e) {
      throw new MetricException(e);
    }
  }

  /**
   * De-register Beam JDBC driver, which produces inconsistent results.
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
