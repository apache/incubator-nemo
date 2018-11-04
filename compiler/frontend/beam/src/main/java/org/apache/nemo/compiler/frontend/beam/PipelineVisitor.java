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
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Traverses through the given Beam pipeline to construct a DAG of Beam Transform,
 * while preserving hierarchy of CompositeTransforms.
 * Hierarchy is established when a CompositeTransform is expanded to other CompositeTransforms or PrimitiveTransforms,
 * as the former CompositeTransform becoming 'enclosingVertex' which have the inner transforms as embedded DAG.
 * This DAG will be later translated by {@link PipelineTranslator} into Nemo IR DAG.
 */
public final class PipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
  private static PipelineTranslator pipelineTranslator = PipelineTranslator.INSTANCE;

  private final PipelineTranslationContext context;

  PipelineVisitor(final Pipeline pipeline, final NemoPipelineOptions pipelineOptions) {
    this.context = new PipelineTranslationContext(pipeline, pipelineOptions);
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
    pipelineTranslator.translatePrimitive(node);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    final CompositeBehavior compositeBehavior = pipelineTranslator.translateComposite(node);

    // this should come after the above translateComposite, since this composite is a child of a previous composite.
    pipelineTranslationContext.enterCompositeTransform(node);
    return compositeBehavior;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    pipelineTranslationContext.leaveCompositeTransform(node);
  }

  /**
   * @return A vertex representing the top-level CompositeTransform.
   */
  DAG<IRVertex, IREdge> getConvertedPipeline() {
    if (rootVertex == null) {
      throw new RuntimeException("The visitor have not fully traversed through a Beam pipeline.");
    }
    return pipelineTranslationContext.getBuilder().build();
  }
}
