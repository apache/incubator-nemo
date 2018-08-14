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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.dag.Edge;
import edu.snu.nemo.common.dag.Vertex;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PValue;

import java.util.*;

/**
 * Traverses through the given Beam pipeline to construct a DAG of Beam Transform hierarchy,
 * which will be later translated by {@link PipelineTranslator}.
 */
public final class PipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  private static final String TRANSFORM = "Transform-";
  private static final String DATAFLOW = "Dataflow-";

  private final Stack<CompositeTransformVertex> compositeTransformVertexStack = new Stack<>();
  private CompositeTransformVertex rootVertex = null;
  private int nextIdx = 0;

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
    final PrimitiveTransformVertex vertex = new PrimitiveTransformVertex(node, compositeTransformVertexStack.peek());
    compositeTransformVertexStack.peek().addVertex(vertex);
    vertex.getPValuesConsumed()
        .forEach(pValue -> getDestinationOfDataFlowEdge(vertex, pValue)
            .parent.addDataFlow(new DataFlow(pValue, vertex)));
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    final CompositeTransformVertex vertex;
    if (compositeTransformVertexStack.isEmpty()) {
      // There is always a top-level CompositeTransform that encompasses the entire Beam pipeline.
      vertex = new CompositeTransformVertex(node, null);
    } else {
      vertex = new CompositeTransformVertex(node, compositeTransformVertexStack.peek());
    }
    compositeTransformVertexStack.push(vertex);
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    final CompositeTransformVertex vertex = compositeTransformVertexStack.pop();
    vertex.build();
    if (compositeTransformVertexStack.isEmpty()) {
      // The vertex is the root.
      if (rootVertex != null) {
        throw new RuntimeException("The visitor already have traversed a Beam pipeline. "
            + "Re-using a visitor is not allowed.");
      }
      rootVertex = vertex;
    } else {
      // The CompositeTransformVertex is ready; adding it to its parent vertex.
      compositeTransformVertexStack.peek().addVertex(vertex);
    }
  }

  public CompositeTransformVertex getConvertedPipeline() {
    if (rootVertex == null) {
      throw new RuntimeException("The visitor have not fully traversed through a Beam pipeline.");
    }
    return rootVertex;
  }
  /**
   * Represents a transform hierarchy for transform.
   */
  public abstract class TransformVertex extends Vertex {
    private final TransformHierarchy.Node node;
    private final CompositeTransformVertex parent;
    private TransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(String.format("%s%d", TRANSFORM, nextIdx++));
      this.node = node;
      this.parent = parent;
    }
    public abstract Collection<PValue> getPValuesProduced();
    public abstract PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue);
    public TransformHierarchy.Node getNode() {
      return node;
    }
    public CompositeTransformVertex getParent() {
      return parent;
    }
  }
  /**
   * Represents a transform hierarchy for primitive transform.
   */
  public final class PrimitiveTransformVertex extends TransformVertex {
    private final List<PValue> pValuesProduced = new ArrayList<>();
    private final List<PValue> pValuesConsumed = new ArrayList<>();
    private PrimitiveTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
      if (node.getTransform() instanceof View.CreatePCollectionView) {
        pValuesProduced.add(((View.CreatePCollectionView) node.getTransform()).getView());
      }
      if (node.getTransform() instanceof ParDo.SingleOutput) {
        pValuesConsumed.addAll(((ParDo.SingleOutput) node.getTransform()).getSideInputs());
      }
      if (node.getTransform() instanceof ParDo.MultiOutput) {
        pValuesConsumed.addAll(((ParDo.MultiOutput) node.getTransform()).getSideInputs());
      }
      pValuesProduced.addAll(getNode().getOutputs().values());
      pValuesConsumed.addAll(getNode().getInputs().values());
    }
    public Collection<PValue> getPValuesProduced() {
      return pValuesProduced;
    }
    public Collection<PValue> getPValuesConsumed() {
      return pValuesConsumed;
    }
    public PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue) {
      if (!getPValuesProduced().contains(pValue)) {
        throw new RuntimeException();
      }
      return this;
    }
  }
  /**
   * Represents a transform hierarchy for composite transform.
   */
  public final class CompositeTransformVertex extends TransformVertex {
    private final Map<PValue, TransformVertex> pValueToProducer = new HashMap<>();
    private final Collection<DataFlow> dataFlows = new ArrayList<>();
    private final DAGBuilder<TransformVertex, DataFlowEdge> builder = new DAGBuilder<>();
    private DAG<TransformVertex, DataFlowEdge> dag = null;
    private CompositeTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
    }
    private void build() {
      if (dag != null) {
        throw new RuntimeException("DAG already have been built.");
      }
      dataFlows.forEach(dataFlow -> {
        final TransformVertex dst = getDestinationOfDataFlowEdge(dataFlow.consumer, dataFlow.pValue);
        builder.connectVertices(new DataFlowEdge(getProducerOf(dataFlow.pValue), dst));
      });
      dag = builder.build();
    }
    private void addVertex(final TransformVertex vertex) {
      vertex.getPValuesProduced().forEach(value -> pValueToProducer.put(value, vertex));
      builder.addVertex(vertex);
    }
    private void addDataFlow(final DataFlow dataFlow) {
      dataFlows.add(dataFlow);
    }
    public Collection<PValue> getPValuesProduced() {
      return pValueToProducer.keySet();
    }
    public TransformVertex getProducerOf(final PValue pValue) {
      final TransformVertex vertex = pValueToProducer.get(pValue);
      if (vertex == null) {
        throw new RuntimeException();
      }
      return vertex;
    }
    public PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue) {
      return getProducerOf(pValue).getPrimitiveProducerOf(pValue);
    }
    public DAG<TransformVertex, DataFlowEdge> getDAG() {
      return dag;
    }
  }
  /**
   * Represents data flow from a transform to another transform.
   */
  public final class DataFlowEdge extends Edge<TransformVertex> {
    private DataFlowEdge(final TransformVertex src,
                         final TransformVertex dst) {
      super(String.format("%s%d", DATAFLOW, nextIdx++), src, dst);
    }

  }

  /**
   * Dataflow.
   */
  private final class DataFlow {
    private final PValue pValue;
    private final TransformVertex consumer;
    private DataFlow(final PValue pValue, final TransformVertex consumer) {
      this.pValue = pValue;
      this.consumer = consumer;
    }
  }

  private TransformVertex getDestinationOfDataFlowEdge(final PrimitiveTransformVertex primitiveConsumer,
                                                       final PValue pValue) {
    TransformVertex current = primitiveConsumer;
    while (true) {
      if (current.getParent().getPValuesProduced().contains(pValue)) {
        return current;
      }
      current = current.getParent();
      if (current.getParent() == null) {
        throw new RuntimeException(String.format("Cannot find producer of %s", pValue));
      }
    }
  }
}
