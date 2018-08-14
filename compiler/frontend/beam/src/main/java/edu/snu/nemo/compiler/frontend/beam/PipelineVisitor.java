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
    vertex.getPValuesConsumed().forEach(pValue -> getCommonParent(vertex, pValue).addDataFlow(pValue));
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    final CompositeTransformVertex vertex;
    if (rootVertex == null) {
      // There is always a top-level CompositeTransform that encompasses the entire Beam pipeline.
      vertex = new CompositeTransformVertex(node, null);
      rootVertex = vertex;
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
    if (!compositeTransformVertexStack.isEmpty()) {
      // The CompositeTransformVertex is ready; adding it to its parent vertex.
      compositeTransformVertexStack.peek().addVertex(vertex);
    }
  }

  public CompositeTransformVertex getRootTransformVertex() {
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
    public abstract Collection<PValue> getPValuesConsumed();
    public abstract PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue);
    public abstract PrimitiveTransformVertex getPrimitiveConsumerOf(final PValue pValue);
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
    public PrimitiveTransformVertex getPrimitiveConsumerOf(final PValue pValue) {
      if (!getPValuesConsumed().contains(pValue)) {
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
    private final Map<PValue, TransformVertex> pValueToConsumer = new HashMap<>();
    private final Collection<PValue> dataFlows = new ArrayList<>();
    private final DAGBuilder<TransformVertex, DataFlowEdge> builder = new DAGBuilder<>();
    private DAG<TransformVertex, DataFlowEdge> dag = null;
    private CompositeTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
    }
    private void build() {
      if (dag != null) {
        throw new RuntimeException("DAG already have been built.");
      }
      dataFlows.forEach(pValue -> builder.connectVertices(new DataFlowEdge(pValue, this)));
      dag = builder.build();
    }
    private void addVertex(final TransformVertex vertex) {
      vertex.getPValuesProduced().forEach(value -> pValueToProducer.put(value, vertex));
      vertex.getPValuesConsumed().forEach(value -> pValueToConsumer.put(value, vertex));
      builder.addVertex(vertex);
    }
    private void addDataFlow(final PValue pValue) {
      dataFlows.add(pValue);
    }
    public Collection<PValue> getPValuesProduced() {
      return pValueToProducer.keySet();
    }
    public Collection<PValue> getPValuesConsumed() {
      return pValueToConsumer.keySet();
    }
    public TransformVertex getProducerOf(final PValue pValue) {
      final TransformVertex vertex = pValueToProducer.get(pValue);
      if (vertex == null) {
        throw new RuntimeException();
      }
      return vertex;
    }
    public TransformVertex getConsumerOf(final PValue pValue) {
      final TransformVertex vertex = pValueToConsumer.get(pValue);
      if (vertex == null) {
        throw new RuntimeException();
      }
      return vertex;
    }
    public PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue) {
      return getProducerOf(pValue).getPrimitiveProducerOf(pValue);
    }
    public PrimitiveTransformVertex getPrimitiveConsumerOf(final PValue pValue) {
      return getConsumerOf(pValue).getPrimitiveConsumerOf(pValue);
    }
    public DAG<TransformVertex, DataFlowEdge> getDAG() {
      return dag;
    }
  }
  /**
   * Represents data flow from a transform to another transform.
   */
  public final class DataFlowEdge extends Edge<TransformVertex> {
    private final PValue pValue;
    private final PrimitiveTransformVertex primitiveProducer;
    private final CompositeTransformVertex mostCloseCommonParent;
    private final PrimitiveTransformVertex primitiveConsumer;

    private DataFlowEdge(final PValue pValue,
                         final CompositeTransformVertex mostCloseCommonParent) {
      super(String.format("%s%d", DATAFLOW, nextIdx++),
          mostCloseCommonParent.getProducerOf(pValue), mostCloseCommonParent.getConsumerOf(pValue));
      this.pValue = pValue;
      this.primitiveProducer = mostCloseCommonParent.getPrimitiveProducerOf(pValue);
      this.mostCloseCommonParent = mostCloseCommonParent;
      this.primitiveConsumer = mostCloseCommonParent.getPrimitiveConsumerOf(pValue);
    }

    public PValue getPValue() {
      return pValue;
    }
    public PrimitiveTransformVertex getPrimitiveProducer() {
      return primitiveProducer;
    }
    public CompositeTransformVertex getMostCloseCommonParent() {
      return mostCloseCommonParent;
    }
    public PrimitiveTransformVertex getPrimitiveConsumer() {
      return primitiveConsumer;
    }
  }

  private static CompositeTransformVertex getCommonParent(final PrimitiveTransformVertex consumer,
                                                          final PValue pValue) {
    CompositeTransformVertex current = consumer.getParent();
    while (true) {
      if (current.getPValuesProduced().contains(pValue)) {
        return current;
      }
      current = current.getParent();
      if (current == null) {
        throw new RuntimeException(String.format("Cannot find producer of %s", pValue));
      }
    }
  }
}
