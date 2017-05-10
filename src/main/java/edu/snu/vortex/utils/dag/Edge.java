/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.utils.dag;

import java.util.logging.Logger;

/**
 * Connects two vertices of a DAG.
 * This class can be extended for various DAG representations.
 *
 * @param <V> the vertex type.
 */
public class Edge<V extends Vertex> {
  private static final Logger LOG = Logger.getLogger(Edge.class.getName());

  private final String id;
  private final V src;
  private final V dst;

  public Edge(final String id, final V src, final V dst) {
    this.id = id;
    this.src = src;
    this.dst = dst;
  }

  public final String getId() {
    return id;
  }

  public final Integer getNumericId() {
    return Integer.parseInt(id.replaceAll("[^\\d.]", ""));
  }

  public final V getSrc() {
    return src;
  }

  public final V getDst() {
    return dst;
  }

  @SuppressWarnings("checkstyle:designforextension")
  public String propertiesToJSON() {
    return "{}";
  }
}
