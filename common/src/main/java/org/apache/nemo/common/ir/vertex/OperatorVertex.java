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
package org.apache.nemo.common.ir.vertex;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IRVertex that transforms input data.
 * It is to be constructed in the compiler frontend with language-specific data transform logic.
 */
public final class OperatorVertex extends IRVertex {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorVertex.class.getName());
  private final Transform transform;

  /**
   * Constructor of OperatorVertex.
   * @param t transform for the OperatorVertex.
   */
  public OperatorVertex(final Transform t) {
    super();
    this.transform = t;
    LOG.info("{} is {}", getId(), transform.toString());
  }

  /**
   * Copy Constructor of OperatorVertex.
   * @param that the source object for copying
   */
  public OperatorVertex(final OperatorVertex that) {
    super();
    this.transform = that.transform;
  }

  @Override
  public OperatorVertex getClone() {
    return new OperatorVertex(this);
  }

  /**
   * @return the transform in the OperatorVertex.
   */
  public Transform getTransform() {
    return transform;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("transform", transform.toString());
    return node;
  }
}
