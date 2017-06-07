/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.client.beam.LoopCompositeTransform;
import edu.snu.vortex.compiler.frontend.beam.coder.BeamCoder;
import edu.snu.vortex.compiler.frontend.beam.transform.*;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Visits every node in the beam dag to translate the BEAM program to the Vortex IR.
 */
final class Visitor extends Pipeline.PipelineVisitor.Defaults {
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Map<PValue, IRVertex> pValueToVertex;
  private final PipelineOptions options;
  // loopVertexStack keeps track of where the beam program is: whether it is inside a composite transform or it is not.
  private final Stack<LoopVertex> loopVertexStack;
  private final Map<PValue, BeamCoder> pValueToCoder;

  /**
   * Constructor of the BEAM Visitor.
   * @param builder DAGBuilder to build the DAG with.
   * @param options Pipeline options.
   */
  Visitor(final DAGBuilder<IRVertex, IREdge> builder, final PipelineOptions options) {
    this.builder = builder;
    this.pValueToVertex = new HashMap<>();
    this.options = options;
    this.loopVertexStack = new Stack<>();
    this.pValueToCoder = new HashMap<>();
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
//    LOG.log(Level.INFO, "visitp " + beamNode.getTransform());
    if (beamNode.getOutputs().size() > 1) {
      throw new UnsupportedOperationException(beamNode.toString());
    }

    final IRVertex vortexIRVertex = convertToVertex(beamNode, builder, pValueToVertex, pValueToCoder, options,
        loopVertexStack);
    beamNode.getOutputs().values().stream()
        .filter(v -> v instanceof PCollection).map(v -> (PCollection) v)
        .forEach(output -> pValueToCoder.put(output, new BeamCoder(output.getCoder())));

    beamNode.getOutputs().values().forEach(output -> pValueToVertex.put(output, vortexIRVertex));

    beamNode.getInputs().values().stream()
        .filter(pValueToVertex::containsKey)
        .forEach(pValue -> {
          final IRVertex src = pValueToVertex.get(pValue);
          final BeamCoder coder = pValueToCoder.get(pValue);
          final IREdge edge = new IREdge(getEdgeType(src, vortexIRVertex), src, vortexIRVertex, coder);
          this.builder.connectVertices(edge);
        });
  }

  /**
   * Convert Beam node to Vortex vertex.
   * @param beamNode input beam node.
   * @param builder the DAG builder to add the vertex to.
   * @param pValueToVertex PValue to Vertex map.
   * @param pValueToCoder PValue to Coder map.
   * @param options pipeline options.
   * @param loopVertexStack Stack to get the current loop vertex that the operator vertex will be assigned to.
   * @param <I> input type.
   * @param <O> output type.
   * @return newly created vertex.
   */
  private static <I, O> IRVertex convertToVertex(final TransformHierarchy.Node beamNode,
                                                 final DAGBuilder<IRVertex, IREdge> builder,
                                                 final Map<PValue, IRVertex> pValueToVertex,
                                                 final Map<PValue, BeamCoder> pValueToCoder,
                                                 final PipelineOptions options,
                                                 final Stack<LoopVertex> loopVertexStack) {
    final PTransform beamTransform = beamNode.getTransform();
    final IRVertex vortexIRVertex;
    if (beamTransform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded) beamTransform;
      vortexIRVertex = new BoundedSourceVertex<>(read.getSource());
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof GroupByKey) {
      vortexIRVertex = new OperatorVertex(new GroupByKeyTransform());
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView) beamTransform;
      final BroadcastTransform vortexTransform = new BroadcastTransform(view.getView());
      vortexIRVertex = new OperatorVertex(vortexTransform);
      pValueToVertex.put(view.getView(), vortexIRVertex);
      builder.addVertex(vortexIRVertex, loopVertexStack);
      // Coders for outgoing edges in BroadcastTransform.
      // Since outgoing PValues for BroadcastTransform is PCollectionView, we cannot use PCollection::getCoder to
      // obtain coders.
      final Coder beamInputCoder = beamNode.getInputs().values().stream()
          .filter(v -> v instanceof PCollection).findFirst().map(v -> (PCollection) v).get().getCoder();
      beamNode.getOutputs().values().stream()
          .forEach(output -> pValueToCoder.put(output, getCoderForView(view.getView().getViewFn(), beamInputCoder)));
    } else if (beamTransform instanceof Window) {
      final Window<I> window = (Window<I>) beamTransform;
      final WindowTransform vortexTransform = new WindowTransform(window.getWindowFn());
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof Window.Assign) {
      final Window.Assign<I> window = (Window.Assign<I>) beamTransform;
      final WindowTransform vortexTransform = new WindowTransform(window.getWindowFn());
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof WriteFiles) {
      throw new UnsupportedOperationException(beamTransform.toString());
    } else if (beamTransform instanceof ParDo.SingleOutput) {
      final ParDo.SingleOutput<I, O> parDo = (ParDo.SingleOutput<I, O>) beamTransform;
      final DoTransform vortexTransform = new DoTransform(parDo.getFn(), options);
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
      connectSideInputs(builder, parDo.getSideInputs(), pValueToVertex, pValueToCoder, vortexIRVertex);
    } else if (beamTransform instanceof ParDo.MultiOutput) {
      final ParDo.MultiOutput<I, O> parDo = (ParDo.MultiOutput<I, O>) beamTransform;
      final DoTransform vortexTransform = new DoTransform(parDo.getFn(), options);
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
      connectSideInputs(builder, parDo.getSideInputs(), pValueToVertex, pValueToCoder, vortexIRVertex);
    } else if (beamTransform instanceof Flatten.PCollections) {
      vortexIRVertex = new OperatorVertex(new FlattenTransform());
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else {
      throw new UnsupportedOperationException(beamTransform.toString());
    }
    return vortexIRVertex;
  }

  private static void connectSideInputs(final DAGBuilder<IRVertex, IREdge> builder,
                                        final List<PCollectionView<?>> sideInputs,
                                        final Map<PValue, IRVertex> pValueToVertex,
                                        final Map<PValue, BeamCoder> pValueToCoder,
                                        final IRVertex vortexIRVertex) {
    sideInputs.stream()
        .filter(pValueToVertex::containsKey)
        .forEach(pValue -> {
          final IRVertex src = pValueToVertex.get(pValue);
          final BeamCoder coder = pValueToCoder.get(pValue);
          final IREdge edge =
              new IREdge(getEdgeType(src, vortexIRVertex), src, vortexIRVertex, coder)
                  .setAttr(Attribute.Key.SideInput, Attribute.SideInput);
          builder.connectVertices(edge);
        });
  }

  /**
   * Get appropriate coder for {@link PCollectionView}.
   * @param viewFn {@link ViewFn} from the corresponding {@link View.CreatePCollectionView} transform
   * @param beamInputCoder Beam {@link Coder} for input value to {@link View.CreatePCollectionView}
   * @return appropriate {@link BeamCoder}
   */
  private static BeamCoder getCoderForView(final ViewFn viewFn, final Coder beamInputCoder) {
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
    return new BeamCoder(beamOutputCoder);
  }

  /**
   * Get the edge type for the src, dst vertex.
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the appropriate edge type.
   */
  private static IREdge.Type getEdgeType(final IRVertex src, final IRVertex dst) {
    if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform) {
      return IREdge.Type.ScatterGather;
    } else if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof BroadcastTransform) {
      return IREdge.Type.Broadcast;
    } else {
      return IREdge.Type.OneToOne;
    }
  }
}
