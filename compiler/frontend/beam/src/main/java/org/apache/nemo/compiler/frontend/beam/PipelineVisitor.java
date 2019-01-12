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
 * Uses the translator and the context to build a Nemo IR DAG.
 * - Translator: Translates each PTransform, and lets us know whether or not to enter into a composite PTransform.
 * - Context: The translator builds a DAG in the context.
 */
public final class PipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
  private static PipelineTranslator pipelineTranslator = PipelineTranslator.INSTANCE;
  private final PipelineTranslationContext context;

  /**
   * @param pipeline to visit.
   * @param pipelineOptions pipeline options.
   */
  PipelineVisitor(final Pipeline pipeline, final NemoPipelineOptions pipelineOptions) {
    this.context = new PipelineTranslationContext(pipeline, pipelineOptions);
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
    pipelineTranslator.translatePrimitive(context, node);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    final CompositeBehavior compositeBehavior = pipelineTranslator.translateComposite(context, node);

    // this should come after the above translateComposite, since this composite is a child of a previous composite.
    context.enterCompositeTransform(node);
    return compositeBehavior;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    context.leaveCompositeTransform(node);
  }

  /**
   * @return the converted pipeline.
   */
  DAG<IRVertex, IREdge> getConvertedPipeline() {
    return context.getBuilder().build();
  }
}
