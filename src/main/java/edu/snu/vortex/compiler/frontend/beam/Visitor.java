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
import edu.snu.vortex.compiler.frontend.beam.transform.*;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

import java.util.HashMap;
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

  /**
   * Constructor of the BEAM Visitor.
   * @param builder DAGBuilder to build the DAG with.
   * @param options Pipeline Options.
   */
  Visitor(final DAGBuilder<IRVertex, IREdge> builder, final PipelineOptions options) {
    this.builder = builder;
    this.pValueToVertex = new HashMap<>();
    this.options = options;
    this.loopVertexStack = new Stack<>();
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

    final IRVertex vortexIRVertex = convertToVertex(beamNode, builder, pValueToVertex, options, loopVertexStack);

    beamNode.getOutputs().forEach(output -> pValueToVertex.put(output.getValue(), vortexIRVertex));

    beamNode.getInputs().stream()
        .map(TaggedPValue::getValue)
        .filter(pValueToVertex::containsKey)
        .map(pValueToVertex::get)
        .forEach(src -> {
          final IREdge edge = new IREdge(getEdgeType(src, vortexIRVertex), src, vortexIRVertex);
          this.builder.connectVertices(edge);
        });
  }

  /**
   * Convert Beam node to Vortex vertex.
   * @param beamNode input beam node.
   * @param builder the DAG builder to add the vertex to.
   * @param pValueToVertex PValue to Vertex map.
   * @param options pipeline options.
   * @param loopVertexStack Stack to get the current loop vertex that the operator vertex will be assigned to.
   * @param <I> input type.
   * @param <O> output type.
   * @return newly created vertex.
   */
  private static <I, O> IRVertex convertToVertex(final TransformHierarchy.Node beamNode,
                                                 final DAGBuilder<IRVertex, IREdge> builder,
                                                 final Map<PValue, IRVertex> pValueToVertex,
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
    } else if (beamTransform instanceof Window.Bound) {
      final Window.Bound<I> window = (Window.Bound<I>) beamTransform;
      final WindowTransform vortexTransform = new WindowTransform(window.getWindowFn());
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof Window.Assign) {
      final Window.Assign<I> window = (Window.Assign<I>) beamTransform;
      final WindowTransform vortexTransform = new WindowTransform(window.getWindowFn());
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else if (beamTransform instanceof Write) {
      throw new UnsupportedOperationException(beamTransform.toString());
    } else if (beamTransform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>) beamTransform;
      final DoTransform vortexTransform = new DoTransform(parDo.getFn(), options);
      vortexIRVertex = new OperatorVertex(vortexTransform);
      builder.addVertex(vortexIRVertex, loopVertexStack);
      parDo.getSideInputs().stream()
          .filter(pValueToVertex::containsKey)
          .map(pValueToVertex::get)
          .forEach(src -> {
            final IREdge edge =
                new IREdge(getEdgeType(src, vortexIRVertex), src, vortexIRVertex)
                    .setAttr(Attribute.Key.SideInput, Attribute.SideInput);
            builder.connectVertices(edge);
          });
    } else if (beamTransform instanceof Flatten.PCollections) {
      vortexIRVertex = new OperatorVertex(new FlattenTransform());
      builder.addVertex(vortexIRVertex, loopVertexStack);
    } else {
      throw new UnsupportedOperationException(beamTransform.toString());
    }
    return vortexIRVertex;
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
