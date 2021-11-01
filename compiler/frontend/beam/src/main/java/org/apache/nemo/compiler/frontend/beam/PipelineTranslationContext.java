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
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.SideInputCoder;
import org.apache.nemo.compiler.frontend.beam.transform.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

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
   * @param pipeline        the pipeline to translate
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

  /**
   * @param compositeTransform composite transform.
   */
  void enterCompositeTransform(final TransformHierarchy.Node compositeTransform) {
    if (compositeTransform.getTransform() instanceof LoopCompositeTransform) {
      final LoopVertex loopVertex = new LoopVertex(compositeTransform.getFullName());
      builder.addVertex(loopVertex, loopVertexStack);
      builder.removeVertex(loopVertex);
      loopVertexStack.push(new LoopVertex(compositeTransform.getFullName()));
    }
  }

  /**
   * @param compositeTransform composite transform.
   */
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
   * Say the dstIRVertex consumes three views: view0, view1, and view2.
   * <p>
   * We translate that as the following:
   * view0 -> SideInputTransform(index=0) ->
   * view1 -> SideInputTransform(index=1) -> dstIRVertex(with a map from indices to PCollectionViews)
   * view2 -> SideInputTransform(index=2) ->
   *
   * @param dstVertex  vertex.
   * @param sideInputs of the vertex.
   */
  void addSideInputEdges(final IRVertex dstVertex, final Map<Integer, PCollectionView<?>> sideInputs) {
    for (final Map.Entry<Integer, PCollectionView<?>> entry : sideInputs.entrySet()) {
      final int index = entry.getKey();
      final PCollectionView view = entry.getValue();

      final IRVertex srcVertex = pValueToProducerVertex.get(view);
      final IRVertex sideInputTransformVertex = new OperatorVertex(new SideInputTransform(index));
      addVertex(sideInputTransformVertex);
      final Coder viewCoder = getCoderForView(view, this);
      final Coder windowCoder = view.getPCollection().getWindowingStrategy().getWindowFn().windowCoder();

      // First edge: view to transform
      final IREdge firstEdge =
        new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, srcVertex, sideInputTransformVertex);
      addEdge(firstEdge, viewCoder, windowCoder);

      // Second edge: transform to the dstIRVertex
      final IREdge secondEdge =
        new IREdge(CommunicationPatternProperty.Value.BROADCAST, sideInputTransformVertex, dstVertex);
      final WindowedValue.FullWindowedValueCoder sideInputElementCoder =
        WindowedValue.getFullCoder(SideInputCoder.of(viewCoder), windowCoder);

      // The vertices should be Parallelism=1
      srcVertex.setPropertyPermanently(ParallelismProperty.of(1));
      sideInputTransformVertex.setPropertyPermanently(ParallelismProperty.of(1));

      secondEdge.setProperty(EncoderProperty.of(new BeamEncoderFactory(sideInputElementCoder)));
      secondEdge.setProperty(DecoderProperty.of(new BeamDecoderFactory(sideInputElementCoder)));
      builder.connectVertices(secondEdge);
    }
  }

  /**
   * Add IR edge to the builder.
   *
   * @param dst   the destination IR vertex.
   * @param input the {@link PValue} {@code dst} consumes
   */
  void addEdgeTo(final IRVertex dst, final PValue input) {
    if (input instanceof PCollection) {
      final Coder elementCoder = ((PCollection) input).getCoder();
      final Coder windowCoder = ((PCollection) input).getWindowingStrategy().getWindowFn().windowCoder();
      final IRVertex src = pValueToProducerVertex.get(input);
      if (src == null) {
        throw new IllegalStateException(String.format("Cannot find a vertex that emits pValue %s", input));
      }

      final CommunicationPatternProperty.Value communicationPattern = getCommPattern(src, dst);
      final IREdge edge = new IREdge(communicationPattern, src, dst);

      if (pValueToTag.containsKey(input)) {
        edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(input).getId()));
      }

      addEdge(edge, elementCoder, windowCoder);
    } else {
      throw new IllegalStateException(input.toString());
    }
  }

  /**
   * @param edge         IR edge to add.
   * @param elementCoder element coder.
   * @param windowCoder  window coder.
   */
  void addEdge(final IREdge edge, final Coder elementCoder, final Coder windowCoder) {
    edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));
    if (elementCoder instanceof KvCoder) {
      Coder keyCoder = ((KvCoder) elementCoder).getKeyCoder();
      edge.setProperty(KeyEncoderProperty.of(new BeamEncoderFactory(keyCoder)));
      edge.setProperty(KeyDecoderProperty.of(new BeamDecoderFactory(keyCoder)));
    }

    final WindowedValue.FullWindowedValueCoder coder = WindowedValue.getFullCoder(elementCoder, windowCoder);
    edge.setProperty(EncoderProperty.of(new BeamEncoderFactory<>(coder)));
    edge.setProperty(DecoderProperty.of(new BeamDecoderFactory<>(coder)));

    builder.connectVertices(edge);
  }

  /**
   * Registers a {@link PValue} as a m.forEach(outputFromGbk -> ain output from the specified {@link IRVertex}.
   *
   * @param node     node
   * @param irVertex the IR vertex
   * @param output   the {@link PValue} {@code irVertex} emits as main output
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
   * @param node     node
   * @param irVertex the IR vertex
   * @param output   the {@link PValue} {@code irVertex} emits as additional output
   * @param tag      the {@link TupleTag} associated with this additional output
   */
  void registerAdditionalOutputFrom(final TransformHierarchy.Node node,
                                    final IRVertex irVertex,
                                    final PValue output,
                                    final TupleTag<?> tag) {
    pValueToProducerBeamNode.put(output, node);
    pValueToTag.put(output, tag);
    pValueToProducerVertex.put(output, irVertex);
  }

  /**
   * @return the pipeline.
   */
  Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * @return the pipeline options.
   */
  PipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  /**
   * @return the dag builder of this translation context.
   */
  DAGBuilder getBuilder() {
    return builder;
  }

  /**
   * @param pValue {@link PValue}
   * @return the producer beam node.
   */
  TransformHierarchy.Node getProducerBeamNodeOf(final PValue pValue) {
    return pValueToProducerBeamNode.get(pValue);
  }

  /**
   * @param src source IR vertex.
   * @param dst destination IR vertex.
   * @return the communication pattern property value.
   */
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
      return CommunicationPatternProperty.Value.SHUFFLE;
    }
    if (srcTransform instanceof FlattenTransform) {
      return CommunicationPatternProperty.Value.ONE_TO_ONE;
    }
    // If GBKTransform represents a partial CombinePerKey transformation, we do NOT need to shuffle its input,
    // since its output will be shuffled before going through a final CombinePerKey transformation.
    if ((dstTransform instanceof CombineTransform && !((CombineTransform) dstTransform).getIsPartialCombining())
      || dstTransform instanceof GroupByKeyTransform) {
      return CommunicationPatternProperty.Value.SHUFFLE;
    }
    if (dstTransform instanceof CreateViewTransform) {
      return CommunicationPatternProperty.Value.BROADCAST;
    }
    return CommunicationPatternProperty.Value.ONE_TO_ONE;
  }

  /**
   * Get appropriate coder for {@link PCollectionView}.
   *
   * @param view    {@link PCollectionView}
   * @param context translation context.
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
      final KvCoder inputValueKVCoder = (KvCoder) inputKVCoder.getValueCoder();
      return MapCoder.of(inputValueKVCoder.getKeyCoder(), inputValueKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
      final KvCoder inputValueKVCoder = (KvCoder) inputKVCoder.getValueCoder();
      return MapCoder.of(inputValueKVCoder.getKeyCoder(), inputValueKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
      return inputKVCoder;
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported viewFn %s", viewFn.getClass()));
    }
  }
}

