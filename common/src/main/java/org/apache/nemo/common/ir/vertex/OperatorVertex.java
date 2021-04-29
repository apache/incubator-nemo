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
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IRVertex that transforms input data.
 * It is to be constructed in the compiler frontend with language-specific data transform logic.
 */
public class OperatorVertex extends IRVertex {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorVertex.class.getName());
  private Transform transform;

  private OperatorVertex partialCombine;
  private OperatorVertex finalCombine;
  private IREdge partialToFinalEdge;

  // for parital vertex !!
  private EncoderFactory originEncoderFactory;
  private Transform partialToFinalTransform;

  /**
   * Constructor of OperatorVertex.
   * @param t transform for the OperatorVertex.
   */
  public OperatorVertex(final Transform t) {
    super();
    this.transform = t;
    this.isPushback = t.isPushback();
    LOG.info("isPushback of {}: {}. {}", getId(), isPushback, t);
  }

  public void setPartialCombine(final OperatorVertex pc) {
    LOG.info("Set partial combine to {}, transform {}", getId(), transform);
    this.partialCombine = pc;
  }

  public void setFinalCombine(final OperatorVertex fc) {
    this.finalCombine = fc;
  }

  public void setPartialToFinalEdge(final IREdge edge) {
    this.partialToFinalEdge = edge;
  }

  public void setOriginEncoderFactory(final EncoderFactory originEncoderFactory) {
    this.originEncoderFactory = originEncoderFactory;
  }

  public EncoderFactory getOriginEncoderFactory() {
    return originEncoderFactory;
  }

  public void setPartialToFinalTransform(final Transform finalTransform) {
    this.partialToFinalTransform = finalTransform;
  }

  public Transform getPartialToFinalTransform() {
    return partialToFinalTransform;
  }

  public OperatorVertex getPartialCombine() {
    return partialCombine;
  }

  public OperatorVertex getFinalCombine() {
    return finalCombine;
  }

  public IREdge getPartialToFinalEdge() {
    return partialToFinalEdge;
  }

  /**
   * Copy Constructor of OperatorVertex.
   * @param that the source object for copying
   */
  public OperatorVertex(final OperatorVertex that) {
    super();
    this.transform = that.transform;
    this.partialToFinalTransform = that.partialToFinalTransform;
    this.partialToFinalEdge = that.partialToFinalEdge;
    this.originEncoderFactory = that.originEncoderFactory;
    this.isGBK = that.isGBK;
    this.isPushback = that.isPushback;
  }

  @Override
  public final OperatorVertex getClone() {
    return new OperatorVertex(this);
  }


  public void setTransform(final Transform tf) {
    this.transform = tf;
  }
  /**
   * @return the transform in the OperatorVertex.
   */
  public final Transform getTransform() {
    return transform;
  }

  @Override
  public final ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("transform", transform.toString());
    return node;
  }

  @Override
  public String toString() {
    return getId();
  }
}
