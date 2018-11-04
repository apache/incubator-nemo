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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import org.apache.nemo.compiler.frontend.beam.transform.*;

import java.util.*;

/**
 * A collection of translators for the Beam PTransforms.
 */

final class PipelineTranslationContext {
  private final PipelineOptions pipelineOptions;
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Map<PValue, TransformHierarchy.Node> pValueToProducerBeamNode;
  private final Map<PValue, IRVertex> pValueToProducerVertex;
  private final Map<PValue, TupleTag<?>> pValueToTag;
  private final Stack<LoopVertex> loopVertexStack;
  private final Pipeline pipeline;

  /**
   * @param pipeline the pipeline to translate
   * @param pipelineOptions {@link PipelineOptions}
   */
  PipelineTranslationContext(final Pipeline pipeline,
                             final PipelineOptions pipelineOptions) {
    this.pipeline = pipeline;
    this.builder = new DAGBuilder<>();
    this.pValueToProducerBeamNode = new HashMap<>();
    this.pValueToProducerVertex = new HashMap<>();
    this.pValueToTag = new HashMap<>();
    this.loopVertexStack = new Stack<>();
    this.pipelineOptions = pipelineOptions;
  }

  void enterCompositeTransform(final TransformHierarchy.Node compositeTransform) {
    if (compositeTransform.getTransform() instanceof LoopCompositeTransform) {
      loopVertexStack.push(new LoopVertex(compositeTransform.getFullName()));
    }
  }

  void leaveCompositeTransform(final TransformHierarchy.Node compositeTransform) {
    if (compositeTransform.getTransform() instanceof LoopCompositeTransform) {
      loopVertexStack.pop();
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
      coder = getCoderForView((PCollectionView) input, this);
    } else {
      throw new RuntimeException(String.format("While adding an edge to %s, coder for PValue %s cannot "
        + "be determined", dst, input));
    }
    addEdgeTo(dst, input, coder);
  }

  void addEdgeTo(final IRVertex dst, final PValue input, final Coder elementCoder) {
    final IRVertex src = pValueToProducerVertex.get(input);
    if (src == null) {
      throw new IllegalStateException(String.format("Cannot find a vertex that emits pValue %s", input));
    }

    final Coder windowCoder;
    final CommunicationPatternProperty.Value communicationPattern = getCommPattern(src, dst);
    final IREdge edge = new IREdge(communicationPattern, src, dst);

    if (pValueToTag.containsKey(input)) {
      edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(input).getId()));
    }
    if (input instanceof PCollectionView) {
      edge.setProperty(BroadcastVariableIdProperty.of((PCollectionView) input));
    }
    if (input instanceof PCollection) {
      windowCoder = ((PCollection) input).getWindowingStrategy().getWindowFn().windowCoder();
    } else if (input instanceof PCollectionView) {
      windowCoder = ((PCollectionView) input).getPCollection()
        .getWindowingStrategy().getWindowFn().windowCoder();
    } else {
      throw new RuntimeException(String.format("While adding an edge from %s, to %s, coder for PValue %s cannot "
        + "be determined", src, dst, input));
    }

    addEdgeTo(edge, elementCoder, windowCoder);
  }

  void addEdgeTo(final IREdge edge,
                 final Coder elementCoder,
                 final Coder windowCoder) {
    edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));

    if (elementCoder instanceof KvCoder) {
      Coder keyCoder = ((KvCoder) elementCoder).getKeyCoder();
      edge.setProperty(KeyEncoderProperty.of(new BeamEncoderFactory(keyCoder)));
      edge.setProperty(KeyDecoderProperty.of(new BeamDecoderFactory(keyCoder)));
    }

    edge.setProperty(EncoderProperty.of(
      new BeamEncoderFactory<>(WindowedValue.getFullCoder(elementCoder, windowCoder))));
    edge.setProperty(DecoderProperty.of(
      new BeamDecoderFactory<>(WindowedValue.getFullCoder(elementCoder, windowCoder))));

    builder.connectVertices(edge);
  }

  /**
   * Registers a {@link PValue} as a m.forEach(outputFromGbk -> ain output from the specified {@link IRVertex}.
   * @param node node
   * @param irVertex the IR vertex
   * @param output the {@link PValue} {@code irVertex} emits as main output
   */
  void registerMainOutputFrom(final TransformHierarchy.Node node,
                              final IRVertex irVertex,
                              final PValue output) {
    pValueToProducerBeamNode.put(output, node);
    pValueToProducerVertex.put(output, irVertex);
  }

  /**
   * Registers a {@link PValue} as an additional output from the specified {@link IRVertex}.
   *
   * @param node node
   * @param irVertex the IR vertex
   * @param output the {@link PValue} {@code irVertex} emits as additional output
   * @param tag the {@link TupleTag} associated with this additional output
   */
  void registerAdditionalOutputFrom(final TransformHierarchy.Node node,
                                    final IRVertex irVertex,
                                    final PValue output,
                                    final TupleTag<?> tag) {
    pValueToProducerBeamNode.put(output, node);
    pValueToTag.put(output, tag);
    pValueToProducerVertex.put(output, irVertex);
  }

  Pipeline getPipeline() {
    return pipeline;
  }

  PipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  DAGBuilder getBuilder() {
    return builder;
  }

  TransformHierarchy.Node getProducerBeamNodeOf(final PValue pValue) {
    return pValueToProducerBeamNode.get(pValue);
  }

  private CommunicationPatternProperty.Value getCommPattern(final IRVertex src, final IRVertex dst) {
    final Class<?> constructUnionTableFn;
    try {
      constructUnionTableFn = Class.forName("org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructUnionTableFn");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    final Transform srcTransform = src instanceof OperatorVertex ? ((OperatorVertex) src).getTransform() : null;
    final Transform dstTransform = dst instanceof OperatorVertex ? ((OperatorVertex) dst).getTransform() : null;
    final DoFn srcDoFn = srcTransform instanceof DoFnTransform ? ((DoFnTransform) srcTransform).getDoFn() : null;

    if (srcDoFn != null && srcDoFn.getClass().equals(constructUnionTableFn)) {
      return CommunicationPatternProperty.Value.Shuffle;
    }
    if (srcTransform instanceof FlattenTransform) {
      return CommunicationPatternProperty.Value.OneToOne;
    }
    if (dstTransform instanceof GroupByKeyAndWindowDoFnTransform
      || dstTransform instanceof GroupByKeyTransform) {
      return CommunicationPatternProperty.Value.Shuffle;
    }
    if (dstTransform instanceof CreateViewTransform) {
      return CommunicationPatternProperty.Value.BroadCast;
    }
    return CommunicationPatternProperty.Value.OneToOne;
  }

  /**
   * Get appropriate coder for {@link PCollectionView}.
   * @param view {@link PCollectionView}
   * @return appropriate {@link Coder} for {@link PCollectionView}
   */
  private static Coder<?> getCoderForView(final PCollectionView view, final PipelineTranslationContext context) {
    final TransformHierarchy.Node src = context.getProducerBeamNodeOf(view);
    final KvCoder<?, ?> inputKVCoder = (KvCoder) src.getOutputs().values().stream()
      .filter(v -> v instanceof PCollection)
      .map(v -> (PCollection) v)
      .findFirst()
      .orElseThrow(() -> new RuntimeException(String.format("No incoming PCollection to %s", src)))
      .getCoder();
    final ViewFn viewFn = view.getViewFn();
    if (viewFn instanceof PCollectionViews.IterableViewFn) {
      return IterableCoder.of(inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.ListViewFn) {
      return ListCoder.of(inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      return MapCoder.of(inputKVCoder.getKeyCoder(), inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
      return MapCoder.of(inputKVCoder.getKeyCoder(), IterableCoder.of(inputKVCoder.getValueCoder()));
    } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
      return inputKVCoder;
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported viewFn %s", viewFn.getClass()));
    }
  }
}

