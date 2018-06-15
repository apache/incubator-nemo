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
package edu.snu.nemo.common.dag;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects two vertices of a DAG.
 * This class can be extended for various DAG representations.
 *
 * @param <V> the vertex type.
 */
public class Edge<V extends Vertex> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Edge.class.getName());

  private final String id;
  private final V src;
  private final V dst;

  /**
   * Constructor for Edge.
   * @param id ID of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   */
  public Edge(final String id, final V src, final V dst) {
    this.id = id;
    this.src = src;
    this.dst = dst;
  }

  /**
   * @return the ID of the edge.
   */
  public final String getId() {
    return id;
  }

  /**
   * @return the numeric ID of the edge.
   */
  public final Integer getNumericId() {
    return Integer.parseInt(id.replaceAll("[^\\d.]", ""));
  }

  /**
   * @return source vertex.
   */
  public final V getSrc() {
    return src;
  }

  /**
   * @return destination vertex.
   */
  public final V getDst() {
    return dst;
  }

  /**
   * @return JSON representation of additional properties
   */
  @SuppressWarnings("checkstyle:designforextension")
  public String propertiesToJSON() {
    return "{}";
  }
}
