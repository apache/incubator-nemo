/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.beam;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.compiler.frontend.beam.source.BeamBoundedSourceVertex;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;

import edu.snu.nemo.compiler.frontend.beam.transform.*;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Visits every node in the beam dag to translate the BEAM program to the IR.
 */
public final class NemoPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Map<PValue, IRVertex> pValueToVertex;
  private final PipelineOptions options;
  // loopVertexStack keeps track of where the beam program is: whether it is inside a composite transform or it is not.
  private final Stack<LoopVertex> loopVertexStack;
  private final Map<PValue, Pair<BeamEncoderFactory, BeamDecoderFactory>> pValueToCoder;
  private final Map<PValue, TupleTag> pValueToTag;

  /**
   * Constructor of the BEAM Visitor.
   *
   * @param builder DAGBuilder to build the DAG with.
   * @param options Pipeline options.
   */
  public NemoPipelineVisitor(final DAGBuilder<IRVertex, IREdge> builder, final PipelineOptions options) {
    this.builder = builder;
    this.pValueToVertex = new HashMap<>();
    this.options = options;
    this.loopVertexStack = new Stack<>();
    this.pValueToCoder = new HashMap<>();
    this.pValueToTag = new HashMap<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node beamNode) {
    if (beamNode.getTransform() instanceof LoopCompositeTransform) {
      final LoopVertex loopVertex = new LoopVertex(beamNode.getFullName());
      this.builder.addVertex(loopVertex, this.loopVertexStack);
      this.builder.removeVertex(loopVertex);
      this.loopVertexStack.push(loopVertex);
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node beamNode) {
    if (beamNode.getTransform() instanceof LoopCompositeTransform) {
      this.loopVertexStack.pop();
    }
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamNode) {
//    Print if needed for development
//    LOG.info("visitp " + beamNode.getTransform());
    final IRVertex irVertex =
        convertToVertex(beamNode, builder, pValueToVertex, pValueToCoder, pValueToTag, options, loopVertexStack);
    beamNode.getOutputs().values().stream().filter(v -> v instanceof PCollection).map(v -> (PCollection) v)
        .forEach(output -> pValueToCoder.put(output,
            Pair.of(new BeamEncoderFactory(output.getCoder()), new BeamDecoderFactory(output.getCoder()))));

    beamNode.getOutputs().values().forEach(output -> pValueToVertex.put(output, irVertex));

    beamNode.getInputs().values().stream().filter(pValueToVertex::containsKey)
        .forEach(pValue -> {
          final boolean isAdditionalOutput = pValueToTag.containsKey(pValue);
          final IRVertex src = pValueToVertex.get(pValue);
          final IREdge edge = new IREdge(getEdgeCommunicationPattern(src, irVertex), src, irVertex, false);
          final Pair<BeamEncoderFactory, BeamDecoderFactory> coderPair = pValueToCoder.get(pValue);
          edge.setProperty(EncoderProperty.of(coderPair.left()));
          edge.setProperty(DecoderProperty.of(coderPair.right()));
          edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));
          // Apply AdditionalOutputTatProperty to edges that corresponds to additional outputs.
          if (isAdditionalOutput) {
            edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(pValue).getId()));
          }
          this.builder.connectVertices(edge);
        });
  }

  /**
   * Convert Beam node to IR vertex.
   *
   * @param beamNode        input beam node.
   * @param builder         the DAG builder to add the vertex to.
   * @param pValueToVertex  PValue to Vertex map.
   * @param pValueToCoder   PValue to EncoderFactory and DecoderFactory map.
   * @param pValueToTag     PValue to Tag map.
   * @param options         pipeline options.
   * @param loopVertexStack Stack to get the current loop vertex that the operator vertex will be assigned to.
   * @param <I>             input type.
   * @param <O>             output type.
   * @return newly created vertex.
   */
  private static <I, O> IRVertex
  convertToVertex(final TransformHierarchy.Node beamNode,
                  final DAGBuilder<IRVertex, IREdge> builder,
                  final Map<PValue, IRVertex> pValueToVertex,
                  final Map<PValue, Pair<BeamEncoderFactory, BeamDecoderFactory>> pValueToCoder,
                  final Map<PValue, TupleTag> pValueToTag,
                  final PipelineOptions options,
                  final Stack<LoopVertex> loopVertexStack) {
    final PTransform beamTransform = beamNode.getTransform();
    final IRVertex irVertex;
    if (beamTransform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded) beamTransform;
      irVertex = new BeamBoundedSourceVertex<>(read.getSource());
      builder.addVertex(irVertex, loopVertexStack);
    } else if (beamTransform instanceof GroupByKey) {
      irVertex = new OperatorVertex(new GroupByKeyTransform());
      builder.addVertex(irVertex, loopVertexStack);
    } else if (beamTransform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView) beamTransform;
      final CreateViewTransform transform = new CreateViewTransform(view.getView());
      irVertex = new OperatorVertex(transform);
      pValueToVertex.put(view.getView(), irVertex);
      builder.addVertex(irVertex, loopVertexStack);
      // Coders for outgoing edges in CreateViewTransform.
      // Since outgoing PValues for CreateViewTransform is PCollectionView,
      // we cannot use PCollection::getEncoderFactory to obtain coders.
      final Coder beamInputCoder = beamNode.getInputs().values().stream()
          .filter(v -> v instanceof PCollection).map(v -> (PCollection) v).findFirst()
          .orElseThrow(() -> new RuntimeException("No inputs provided to " + beamNode.getFullName())).getCoder();
      beamNode.getOutputs().values().stream()
          .forEach(output ->
              pValueToCoder.put(output, getCoderPairForView(view.getView().getViewFn(), beamInputCoder)));
    } else if (beamTransform instanceof Window) {
      final Window<I> window = (Window<I>) beamTransform;
      final WindowTransform transform = new WindowTransform(window.getWindowFn());
      irVertex = new OperatorVertex(transform);
      builder.addVertex(irVertex, loopVertexStack);
    } else if (beamTransform instanceof Window.Assign) {
      final Window.Assign<I> window = (Window.Assign<I>) beamTransform;
      final WindowTransform transform = new WindowTransform(window.getWindowFn());
      irVertex = new OperatorVertex(transform);
      builder.addVertex(irVertex, loopVertexStack);
    } else if (beamTransform instanceof ParDo.SingleOutput) {
      final ParDo.SingleOutput<I, O> parDo = (ParDo.SingleOutput<I, O>) beamTransform;
      final DoTransform transform = new DoTransform(parDo.getFn(), options);
      irVertex = new OperatorVertex(transform);
      builder.addVertex(irVertex, loopVertexStack);
      connectSideInputs(builder, parDo.getSideInputs(), pValueToVertex, pValueToCoder, irVertex);
    } else if (beamTransform instanceof ParDo.MultiOutput) {
      final ParDo.MultiOutput<I, O> parDo = (ParDo.MultiOutput<I, O>) beamTransform;
      final DoTransform transform = new DoTransform(parDo.getFn(), options);
      irVertex = new OperatorVertex(transform);
      if (parDo.getAdditionalOutputTags().size() > 0) {
        beamNode.getOutputs().entrySet().stream()
            .filter(kv -> !kv.getKey().equals(parDo.getMainOutputTag()))
            .forEach(kv -> pValueToTag.put(kv.getValue(), kv.getKey()));
      }
      builder.addVertex(irVertex, loopVertexStack);
      connectSideInputs(builder, parDo.getSideInputs(), pValueToVertex, pValueToCoder, irVertex);
    } else if (beamTransform instanceof Flatten.PCollections) {
      irVertex = new OperatorVertex(new FlattenTransform());
      builder.addVertex(irVertex, loopVertexStack);
    } else {
      throw new UnsupportedOperationException(beamTransform.toString());
    }
    return irVertex;
  }

  /**
   * Connect side inputs to the vertex.
   *
   * @param builder        the DAG builder to add the vertex to.
   * @param sideInputs     side inputs.
   * @param pValueToVertex PValue to Vertex map.
   * @param pValueToCoder  PValue to Encoder/Decoder factory map.
   * @param irVertex       wrapper for a user operation in the IR. (Where the side input is headed to)
   */
  private static void connectSideInputs(final DAGBuilder<IRVertex, IREdge> builder,
                                        final List<PCollectionView<?>> sideInputs,
                                        final Map<PValue, IRVertex> pValueToVertex,
                                        final Map<PValue, Pair<BeamEncoderFactory, BeamDecoderFactory>> pValueToCoder,
                                        final IRVertex irVertex) {
    sideInputs.stream().filter(pValueToVertex::containsKey)
        .forEach(pValue -> {
          final IRVertex src = pValueToVertex.get(pValue);
          final IREdge edge = new IREdge(getEdgeCommunicationPattern(src, irVertex), src, irVertex, true);
          final Pair<BeamEncoderFactory, BeamDecoderFactory> coder = pValueToCoder.get(pValue);
          edge.setProperty(EncoderProperty.of(coder.left()));
          edge.setProperty(DecoderProperty.of(coder.right()));
          edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));
          builder.connectVertices(edge);
        });
  }

  /**
   * Get appropriate encoder and decoder pair for {@link PCollectionView}.
   *
   * @param viewFn         {@link ViewFn} from the corresponding {@link View.CreatePCollectionView} transform
   * @param beamInputCoder Beam {@link Coder} for input value to {@link View.CreatePCollectionView}
   * @return appropriate pair of {@link BeamEncoderFactory} and {@link BeamDecoderFactory}
   */
  private static Pair<BeamEncoderFactory, BeamDecoderFactory> getCoderPairForView(final ViewFn viewFn,
                                                                                  final Coder beamInputCoder) {
    final Coder beamOutputCoder;
    if (viewFn instanceof PCollectionViews.IterableViewFn) {
      beamOutputCoder = IterableCoder.of(beamInputCoder);
    } else if (viewFn instanceof PCollectionViews.ListViewFn) {
      beamOutputCoder = ListCoder.of(beamInputCoder);
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      final KvCoder inputCoder = (KvCoder) beamInputCoder;
      beamOutputCoder = MapCoder.of(inputCoder.getKeyCoder(), inputCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
      final KvCoder inputCoder = (KvCoder) beamInputCoder;
      beamOutputCoder = MapCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder()));
    } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
      beamOutputCoder = beamInputCoder;
    } else {
      throw new UnsupportedOperationException("Unsupported viewFn: " + viewFn.getClass());
    }
    return Pair.of(new BeamEncoderFactory(beamOutputCoder), new BeamDecoderFactory(beamOutputCoder));
  }

  /**
   * Get the edge type for the src, dst vertex.
   *
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the appropriate edge type.
   */
  private static CommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                                final IRVertex dst) {
    final Class<?> constructUnionTableFn;
    try {
      constructUnionTableFn = Class.forName("org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructUnionTableFn");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    final Transform srcTransform = src instanceof OperatorVertex ? ((OperatorVertex) src).getTransform() : null;
    final Transform dstTransform = dst instanceof OperatorVertex ? ((OperatorVertex) dst).getTransform() : null;
    final DoFn srcDoFn = srcTransform instanceof DoTransform ? ((DoTransform) srcTransform).getDoFn() : null;

    if (srcDoFn != null && srcDoFn.getClass().equals(constructUnionTableFn)) {
      return CommunicationPatternProperty.Value.Shuffle;
    }
    if (srcTransform instanceof FlattenTransform) {
      return CommunicationPatternProperty.Value.OneToOne;
    }
    if (dstTransform instanceof GroupByKeyTransform) {
      return CommunicationPatternProperty.Value.Shuffle;
    }
    if (dstTransform instanceof CreateViewTransform) {
      return CommunicationPatternProperty.Value.BroadCast;
    }
    return CommunicationPatternProperty.Value.OneToOne;
  }
}
