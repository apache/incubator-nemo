/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.translator.beam;

import edu.snu.vortex.translator.beam.node.BroadcastImpl;
import edu.snu.vortex.translator.beam.node.DoImpl;
import edu.snu.vortex.translator.beam.node.SourceImpl;
import edu.snu.vortex.compiler.plan.DAGBuilder;
import edu.snu.vortex.compiler.plan.Edge;
import edu.snu.vortex.compiler.plan.node.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;
import java.util.Map;

class Visitor implements Pipeline.PipelineVisitor {
  private final DAGBuilder builder;
  private final Map<PValue, Node> pValueToNodeOutput;

  Visitor(final DAGBuilder builder) {
    this.builder = builder;
    this.pValueToNodeOutput = new HashMap<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    // Print if needed for development
    // System.out.println("enter composite " + node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    // Print if needed for development
    // System.out.println("leave composite " + node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamNode) {
    // Print if needed for development
    // System.out.println("visitp " + beamNode.getTransform());
    if (beamNode.getOutputs().size() > 1 || beamNode.getInputs().size() > 1)
      throw new UnsupportedOperationException(beamNode.toString());

    final Node newNode = createNode(beamNode);
    builder.addNode(newNode);

    beamNode.getOutputs()
        .forEach(output -> pValueToNodeOutput.put(output, newNode));

    beamNode.getInputs().stream()
        .filter(pValueToNodeOutput::containsKey)
        .map(pValueToNodeOutput::get)
        .forEach(src -> builder.connectNodes(src, newNode, getInEdgeType(newNode)));
  }

  @Override
  public void visitValue(final PValue value, final TransformHierarchy.Node  producer) {
    // Print if needed for development
    // System.out.println("visitv value " + value);
    // System.out.println("visitv producer " + producer.getTransform());
  }

  private <I, O> Node createNode(final TransformHierarchy.Node beamNode) {
    final PTransform transform = beamNode.getTransform();
    if (transform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded)transform;
      final SourceImpl<O> source = new SourceImpl<>(read.getSource());
      return source;
    } else if (transform instanceof GroupByKey) {
      return new edu.snu.vortex.compiler.plan.node.GroupByKey();
    } else if (transform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView)transform;
      final Broadcast newNode = new BroadcastImpl(view.getView());
      pValueToNodeOutput.put(view.getView(), newNode);
      return newNode;
    } else if (transform instanceof Write.Bound) {
      throw new UnsupportedOperationException(transform.toString());
    } else if (transform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>)transform;
      final DoImpl<I, O> newNode = new DoImpl<>(parDo.getNewFn());
      parDo.getSideInputs().stream()
          .filter(pValueToNodeOutput::containsKey)
          .map(pValueToNodeOutput::get)
          .forEach(src -> builder.connectNodes(src, newNode, Edge.Type.O2O)); // Broadcasted = O2O
      return newNode;
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }

  private Edge.Type getInEdgeType(final Node node) {
    if (node instanceof edu.snu.vortex.compiler.plan.node.GroupByKey) {
      return Edge.Type.M2M;
    } else if (node instanceof Broadcast) {
      return Edge.Type.O2M;
    } else {
      return Edge.Type.O2O;
    }
  }
}
