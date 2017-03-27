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

import edu.snu.vortex.compiler.frontend.beam.transform.BroadcastTransform;
import edu.snu.vortex.compiler.frontend.beam.transform.DoTransform;
import edu.snu.vortex.compiler.frontend.beam.transform.GroupByKeyTransform;
import edu.snu.vortex.compiler.frontend.beam.transform.WindowTransform;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Visits every node in the beam dag to translate the BEAM program to the Vortex IR.
 */
final class Visitor extends Pipeline.PipelineVisitor.Defaults {
  private final DAGBuilder builder;
  private final Map<PValue, Vertex> pValueToVertex;
  private final PipelineOptions options;

  Visitor(final DAGBuilder builder, final PipelineOptions options) {
    this.builder = builder;
    this.pValueToVertex = new HashMap<>();
    this.options = options;
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamNode) {
//    Print if needed for development
//    System.out.println("visitp " + beamNode.getTransform());
    if (beamNode.getOutputs().size() > 1 || beamNode.getInputs().size() > 1) {
      throw new UnsupportedOperationException(beamNode.toString());
    }

    final Vertex vortexVertex = convertToVertex(beamNode);
    builder.addVertex(vortexVertex);

    beamNode.getOutputs()
        .forEach(output -> pValueToVertex.put(output, vortexVertex));

    if (vortexVertex instanceof OperatorVertex) {
      beamNode.getInputs().stream()
          .filter(pValueToVertex::containsKey)
          .map(pValueToVertex::get)
          .forEach(src -> builder.connectVertices(src, vortexVertex, getEdgeType(src, vortexVertex)));
    }
  }

  /**
   * Convert Beam node to Vortex vertex.
   * @param beamNode input beam node.
   * @param <I> input type.
   * @param <O> output type.
   * @return newly created vertex.
   */
  private <I, O> Vertex convertToVertex(final TransformHierarchy.Node beamNode) {
    final PTransform beamTransform = beamNode.getTransform();
    if (beamTransform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded) beamTransform;
      return new BoundedSourceVertex<>(read.getSource());
    } else if (beamTransform instanceof GroupByKey) {
      return new OperatorVertex(new GroupByKeyTransform());
    } else if (beamTransform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView) beamTransform;
      final BroadcastTransform vortexTransform = new BroadcastTransform(view.getView());
      final Vertex vortexVertex = new OperatorVertex(vortexTransform);
      pValueToVertex.put(view.getView(), vortexVertex);
      return vortexVertex;
    } else if (beamTransform instanceof Window.Bound) {
      final Window.Bound<I> window = (Window.Bound<I>) beamTransform;
      final WindowTransform vortexTransform = new WindowTransform(window.getWindowFn());
      return new OperatorVertex(vortexTransform);
    } else if (beamTransform instanceof Write.Bound) {
      throw new UnsupportedOperationException(beamTransform.toString());
    } else if (beamTransform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>) beamTransform;
      final DoTransform vortexTransform = new DoTransform(parDo.getNewFn(), options);
      final Vertex vortexVertex = new OperatorVertex(vortexTransform);
      parDo.getSideInputs().stream()
          .filter(pValueToVertex::containsKey)
          .map(pValueToVertex::get)
          .forEach(src -> builder.connectVertices(src, vortexVertex, getEdgeType(src, vortexVertex))
              .setAttr(Attribute.Key.SideInput, Attribute.SideInput));
      return vortexVertex;
    } else {
      throw new UnsupportedOperationException(beamTransform.toString());
    }
  }

  private Edge.Type getEdgeType(final Vertex src, final Vertex dst) {
    if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform) {
      return Edge.Type.ScatterGather;
    } else if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof BroadcastTransform) {
      return Edge.Type.Broadcast;
    } else {
      return Edge.Type.OneToOne;
    }
  }
}
