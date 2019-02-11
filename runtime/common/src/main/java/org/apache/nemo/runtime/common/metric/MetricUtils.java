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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utility class for metrics.
 */
public final class MetricUtils {
  private static final Map<Integer, Pair<String, Class<? extends ExecutionProperty>>>
    HASHCODE_TO_ID_AND_EP_KEY = new HashMap<>();
  private static final Map<Integer, ExecutionProperty<?>> HASHCODE_TO_EXECUTION_PROPERTY = new HashMap<>();

  /**
   * Private constructor.
   */
  private MetricUtils() {
  }

  /**
   * Stringify vertex execution properties of an IR DAG.
   * @param irdag IR DAG to observe the vertices of.
   * @return the stringified execution properties.
   */
  static String stringifyVertexProperties(final IRDAG irdag) {
    final StringBuilder builder = new StringBuilder();
    irdag.getVertices().forEach(v ->
      v.getExecutionProperties().forEachProperties(ep ->
        epFormatter(builder, v.getId(), v.getNumericId(), ep)));
    return builder.toString().trim();
  }

  /**
   * Stringify edge execution properties of an IR DAG.
   * @param irdag IR DAG to observe the edges of.
   * @return the stringified execution properties.
   */
  static String stringifyEdgeProperties(final IRDAG irdag) {
    final StringBuilder builder = new StringBuilder();
    irdag.getVertices().forEach(v ->
      irdag.getIncomingEdgesOf(v).forEach(e ->
        e.getExecutionProperties().forEachProperties(ep ->
          epFormatter(builder, e.getId(), e.getNumericId(), ep))));
    return builder.toString().trim();
  }

  /**
   * Formatter for execution properties.
   * @param builder string builder to append the metrics to.
   * @param id ID of the vertex or the edge.
   * @param numericId numeric ID of the vertex or the edge.
   * @param ep the execution property.
   */
  private static void epFormatter(final StringBuilder builder, final String id,
                                  final Integer numericId, final ExecutionProperty<?> ep) {
    builder.append(numericId);
    builder.append("0");
    final int classHashcode = ep.getClass().hashCode();
    HASHCODE_TO_ID_AND_EP_KEY.putIfAbsent(classHashcode, Pair.of(id, ep.getClass()));
    builder.append(classHashcode);

    builder.append(":");
    final int valueHashcode = ep.hashCode();
    HASHCODE_TO_EXECUTION_PROPERTY.putIfAbsent(valueHashcode, ep);
    builder.append(valueHashcode);
    builder.append(" ");
  }

  /**
   * Get the vertex/edge id and the execution property class (key) by the hashcode.
   * @param hashcode the hashcode.
   * @return the pair of id and the execution property.
   */
  public static Pair<String, Class<? extends ExecutionProperty>> getIdAndEPKeyByHashcode(final int hashcode) {
    return HASHCODE_TO_ID_AND_EP_KEY.get(hashcode);
  }

  /**
   * Get the execution property by the hashcode.
   * @param hashcode the hashcode.
   * @return the execution property.
   */
  public static ExecutionProperty<?> getExecutionPropertyByHashcode(final int hashcode) {
    return HASHCODE_TO_EXECUTION_PROPERTY.get(hashcode);
  }

  /**
   * Finds the project root path by looking for the travis.yml file.
   * @return the project root path.
   */
  public static String fetchProjectRootPath() {
    return recursivelyFindTravis(Paths.get(System.getProperty("user.dir")));
  }

  /**
   * Helper method to recursively find the travis.yml file.
   * @param path the path to search for.
   * @return the path containing the travis.yml file.
   */
  private static String recursivelyFindTravis(final Path path) {
    try (final Stream stream = Files.find(path, 1, (p, attributes) -> p.endsWith(".travis.yml"))) {
      if (stream.count() > 0) {
        return path.toAbsolutePath().toString();
      } else {
        return recursivelyFindTravis(path.getParent());
      }
    } catch (IOException e) {
      throw new MetricException(e);
    }
  }
}
