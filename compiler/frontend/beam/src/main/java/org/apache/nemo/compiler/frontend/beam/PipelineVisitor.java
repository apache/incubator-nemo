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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

import java.util.*;

/**
 * Traverses through the given Beam pipeline to construct a DAG of Beam Transform,
 * while preserving hierarchy of CompositeTransforms.
 * Hierarchy is established when a CompositeTransform is expanded to other CompositeTransforms or PrimitiveTransforms,
 * as the former CompositeTransform becoming 'enclosingVertex' which have the inner transforms as embedded DAG.
 * This DAG will be later translated by {@link PipelineTranslator} into Nemo IR DAG.
 */
public final class PipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
  private final Stack<TransformHierarchy.Node> compositeTransformVertexStack = new Stack<>();
  private TransformHierarchy.Node rootVertex = null;
  private static PipelineTranslationContext pipelineTranslationContext = new PipelineTranslationContext();

  public PipelineVisitor() {
    final PipelineTranslationContext ctx = new PipelineTranslationContext(vertex, pipeline, primitiveTransformToTranslator,
      compositeTransformToTranslator, DefaultCommunicationPatternSelector.INSTANCE, pipelineOptions);

  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
    compositeTransformVertexStack.peek().addVertex(vertex);
    vertex.getPValuesConsumed()
        .forEach(pValue -> {
          final TransformVertex dst = getDestinationOfDataFlowEdge(vertex, pValue);
          dst.enclosingVertex.addDataFlow(new DataFlowEdge(dst.enclosingVertex.getProducerOf(pValue), dst));
        });
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    final TransformHierarchy.Node vertex;
    if (compositeTransformVertexStack.isEmpty()) {
      // There is always a top-level CompositeTransform that encompasses the entire Beam pipeline.
      vertex = new TransformHierarchy.Node(node, null);
    } else {
      vertex = new TransformHierarchy.Node(node, compositeTransformVertexStack.peek());
    }
    compositeTransformVertexStack.push(vertex);
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    final TransformHierarchy.Node vertex = compositeTransformVertexStack.pop();
    vertex.build();
    if (compositeTransformVertexStack.isEmpty()) {
      // The vertex is the root.
      if (rootVertex != null) {
        throw new RuntimeException("The visitor already have traversed a Beam pipeline. "
            + "Re-using a visitor is not allowed.");
      }
      rootVertex = vertex;
    } else {
      // The TransformHierarchy.Node is ready; adding it to its enclosing vertex.
      compositeTransformVertexStack.peek().addVertex(vertex);
    }
  }

  /**
   * @return A vertex representing the top-level CompositeTransform.
   */
  public TransformHierarchy.Node getConvertedPipeline() {
    if (rootVertex == null) {
      throw new RuntimeException("The visitor have not fully traversed through a Beam pipeline.");
    }
    return rootVertex;
  }
}
