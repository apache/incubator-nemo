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
package org.apache.nemo.common.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.Serializable;

/**
 * A vertex in DAG.
 */
public abstract class Vertex implements Serializable {
  private final String id;

  /**
   * @param id unique identifier of the vertex
   */
  public Vertex(final String id) {
    this.id = id;
  }

  /**
   * @return identifier of the vertex
   */
  public final String getId() {
    return id;
  }
  /**
   * @return the numeric id of the vertex.
   */
  public final Integer getNumericId() {
    return Integer.parseInt(id.replaceAll("[^\\d.]", ""));
  }

  /**
   * @return JSON representation of additional properties
   */
  @SuppressWarnings("checkstyle:designforextension")
  public JsonNode getPropertiesAsJsonNode() {
    return JsonNodeFactory.instance.objectNode();
  }
}
