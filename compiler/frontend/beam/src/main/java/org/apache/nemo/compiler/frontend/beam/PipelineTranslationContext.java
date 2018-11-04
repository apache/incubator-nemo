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
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiFunction;

/**
 * A collection of translators for the Beam PTransforms.
 */

final class PipelineTranslationContext {
  private final TransformHierarchy.Node root;
  private final Stack<TransformHierarchy.Node> compositeTransformStack;

  private final PipelineOptions pipelineOptions;
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Map<PValue, IRVertex> pValueToProducer;
  private final Map<PValue, TupleTag<?>> pValueToTag;
  private final Stack<LoopVertex> loopVertexStack;
  private final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> communicationPatternSelector;
  private final Pipeline pipeline;


  private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator;
  private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator;

  /**
   * @param root the root to translate
   * @param pipeline the pipeline to translate
   * @param primitiveTransformToTranslator provides translators for PrimitiveTransform
   * @param compositeTransformToTranslator provides translators for CompositeTransform
   * @param selector provides {@link CommunicationPatternProperty.Value} for IR edges
   * @param pipelineOptions {@link PipelineOptions}
   */
  PipelineTranslationContext(final TransformHierarchy.Node root,
                             final Pipeline pipeline,
                             final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator,
                             final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator,
                             final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> selector,
                             final PipelineOptions pipelineOptions) {
    this.root = root;
    this.hierarchyStack = new Stack<>();

    this.pipeline = pipeline;
    this.builder = new DAGBuilder<>();
    this.pValueToProducer = new HashMap<>();
    this.pValueToTag = new HashMap<>();
    this.loopVertexStack = new Stack<>();
    this.primitiveTransformToTranslator = primitiveTransformToTranslator;
    this.compositeTransformToTranslator = compositeTransformToTranslator;
    this.communicationPatternSelector = selector;
    this.pipelineOptions = pipelineOptions;
  }

  void enterCompositeTransform(final TransformHierarchy.Node compositeTransform) {
    compositeTransformStack.push(compositeTransform);
  }

  void leaveCompositeTransform(final TransformHierarchy.Node compositeTransform) {
    if (compositeTransform.equals(compositeTransformStack.pop())) {
      throw new IllegalStateException(compositeTransform.toString());
    }
  }


  /**
   * Add IR vertex to the builder.
   *
   * @param vertex IR vertex to add
   */
  void addVertex(final IRVertex vertex) {
    builder.addVertex(vertex, loopVertexStack);
  }

  /**
   * Add IR edge to the builder.
   *
   * @param dst the destination IR vertex.
   * @param input the {@link PValue} {@code dst} consumes
   */
  void addEdgeTo(final IRVertex dst, final PValue input) {
    final Coder coder;
    if (input instanceof PCollection) {
      coder = ((PCollection) input).getCoder();
    } else if (input instanceof PCollectionView) {
      coder = getCoderForView((PCollectionView) input, root);
    } else {
      throw new RuntimeException(String.format("While adding an edge to %s, coder for PValue %s cannot "
        + "be determined", dst, input));
    }
    addEdgeTo(dst, input, coder);
  }

  void addEdgeTo(final IRVertex dst, final PValue input, final Coder coder) {
    final IRVertex src = pValueToProducer.get(input);
    if (src == null) {
      try {
        throw new RuntimeException(String.format("Cannot find a vertex that emits pValue %s, "
          + "while PTransform %s is known to produce it.", input, root.getPrimitiveProducerOf(input)));
      } catch (final RuntimeException e) {
        throw new RuntimeException(String.format("Cannot find a vertex that emits pValue %s, "
          + "and the corresponding PTransform was not found", input));
      }
    }
    final CommunicationPatternProperty.Value communicationPattern = communicationPatternSelector.apply(src, dst);
    if (communicationPattern == null) {
      throw new RuntimeException(String.format("%s have failed to determine communication pattern "
        + "for an edge from %s to %s", communicationPatternSelector, src, dst));
    }
    final IREdge edge = new IREdge(communicationPattern, src, dst);

    if (pValueToTag.containsKey(input)) {
      edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(input).getId()));
    }

    if (input instanceof PCollectionView) {
      edge.setProperty(BroadcastVariableIdProperty.of((PCollectionView) input));
    }

    edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));

    if (coder instanceof KvCoder) {
      Coder keyCoder = ((KvCoder) coder).getKeyCoder();
      edge.setProperty(KeyEncoderProperty.of(new BeamEncoderFactory(keyCoder)));
      edge.setProperty(KeyDecoderProperty.of(new BeamDecoderFactory(keyCoder)));
    }

    final Coder windowCoder = ((PCollection) input).getWindowingStrategy().getWindowFn().windowCoder();
    edge.setProperty(EncoderProperty.of(
      new BeamEncoderFactory<>(WindowedValue.getFullCoder(coder, windowCoder))));
    edge.setProperty(DecoderProperty.of(
      new BeamDecoderFactory<>(WindowedValue.getFullCoder(coder, windowCoder))));

    builder.connectVertices(edge);
  }

  /**
   * Registers a {@link PValue} as a m.forEach(outputFromGbk -> ain output from the specified {@link IRVertex}.
   *
   * @param irVertex the IR vertex
   * @param output the {@link PValue} {@code irVertex} emits as main output
   */
  void registerMainOutputFrom(final IRVertex irVertex, final PValue output) {
    pValueToProducer.put(output, irVertex);
  }

  /**
   * Registers a {@link PValue} as an additional output from the specified {@link IRVertex}.
   *
   * @param irVertex the IR vertex
   * @param output the {@link PValue} {@code irVertex} emits as additional output
   * @param tag the {@link TupleTag} associated with this additional output
   */
  void registerAdditionalOutputFrom(final IRVertex irVertex, final PValue output, final TupleTag<?> tag) {
    pValueToTag.put(output, tag);
    pValueToProducer.put(output, irVertex);
  }

  Pipeline getPipeline() {
    return pipeline;

  }
}

