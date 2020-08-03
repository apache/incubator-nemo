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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.dag.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID manager.
 */
public final class IdManager {
  /**
   * Private constructor.
   */
  private IdManager() {
  }

  private static AtomicInteger vertexId = new AtomicInteger(1);
  private static AtomicInteger edgeId = new AtomicInteger(1);
  private static AtomicLong resourceSpecIdGenerator = new AtomicLong(0);
  private static AtomicInteger duplicatedEdgeGroupId = new AtomicInteger(0);
  private static AtomicInteger messageId = new AtomicInteger(1);
  private static volatile boolean isDriver = false;

  // Vertex ID Map to be used upon cloning in loop vertices.
  private static final Map<Vertex, Queue<String>> VERTEX_ID_MAP = new HashMap<>();

  /**
   * @return a new operator ID.
   */
  public static String newVertexId() {
    return "vertex" + (isDriver ? "(d)" : "") + vertexId.getAndIncrement();
  }

  /**
   * Save the vertex id for the vertices that can be cloned later on.
   * WARN: this should guarantee that the vertex is no longer used, otherwise, it would result in duplicate IDs.
   *
   * @param v  the original vertex that is to be cloned later on (RootLoopVertex's vertex).
   * @param id The IDs of the identical vertices.
   */
  public static void saveVertexId(final Vertex v, final String id) {
    VERTEX_ID_MAP.putIfAbsent(v, new LinkedBlockingQueue<>());
    VERTEX_ID_MAP.get(v).add(id);
  }

  /**
   * Used for cloning vertices. If an existing ID exists, it returns the unused ID,
   * otherwise simply acts as the newVertexId method.
   * WARN: the #saveVertexId method should no longer use the ID saved at that moment,
   * in order for this method to work correctly.
   *
   * @param v the vertex to get the ID for.
   * @return the ID for the vertex.
   */
  public static String getVertexId(final Vertex v) {
    final Queue<String> idQueue = VERTEX_ID_MAP.getOrDefault(v, null);
    if (idQueue == null) {
      return newVertexId();
    } else {
      final String id = idQueue.poll();
      return id != null ? id : newVertexId();
    }
  }

  /**
   * @return a new edge ID.
   */
  public static String newEdgeId() {
    return "edge" + (isDriver ? "(d)" : "") + edgeId.getAndIncrement();
  }

  /**
   * Generates the ID for a resource specification.
   *
   * @return the generated ID
   */
  public static String generateResourceSpecId() {
    return "ResourceSpec" + resourceSpecIdGenerator.getAndIncrement();
  }

  public static Integer generateDuplicatedEdgeGroupId() {
    return duplicatedEdgeGroupId.getAndIncrement();
  }

  public static Integer generateMessageId() {
    return messageId.getAndIncrement();
  }
  /**
   * Set the realm of the loaded class as REEF driver.
   */
  public static void setInDriver() {
    isDriver = true;
  }
}
