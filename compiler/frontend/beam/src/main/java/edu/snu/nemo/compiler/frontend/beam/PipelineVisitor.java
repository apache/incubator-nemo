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
import org.apache.beam.sdk.values.PValue;

import java.util.*;

/**
 * Traverses through the given Beam pipeline to construct a DAG of Beam Transform hierarchy,
 * which will be later translated by {@link PipelineTranslator}.
 */
public final class PipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  private final Stack<CompositeTransformVertex> compositeTransformVertexStack = new Stack<>();
  private CompositeTransformVertex rootVertex = null;

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
    final PrimitiveTransformVertex vertex = new PrimitiveTransformVertex(node, compositeTransformVertexStack.peek());
    compositeTransformVertexStack.peek().addVertex(vertex);
    vertex.getPValuesConsumed().forEach(pValue -> {
      final PrimitiveTransformVertex primitiveConsumer = vertex;
      final CompositeTransformVertex mostCloseCommonParent = getCommonParent(vertex, pValue);
      final PrimitiveTransformVertex primitiveProducer = mostCloseCommonParent.getPrimitiveProducerOf(pValue);
      final DataFlowEdge edge = new DataFlowEdge(pValue, primitiveProducer, mostCloseCommonParent, primitiveConsumer);
      mostCloseCommonParent.addEdge(edge);
    });
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

  /**
   * @return DAG of Beam Transform hierarchy.
   */
  public DAG<TransformVertex, DataFlowEdge> buildDAG() {
    if (!compositeTransformVertexStack.isEmpty()) {
      throw new RuntimeException("The visitor haven't left from the root node yet.");
    }
    return rootVertex.getDAG();
  }
  /**
   * Represents a transform hierarchy for transform.
   */
  public abstract static class TransformVertex extends Vertex {
    private final TransformHierarchy.Node node;
    private final CompositeTransformVertex parent;
    private TransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(UUID.randomUUID().toString());
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
  public static final class PrimitiveTransformVertex extends TransformVertex {
    private PrimitiveTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
    }
    public Collection<PValue> getPValuesProduced() {
      return getNode().getOutputs().values();
    }
    public Collection<PValue> getPValuesConsumed() {
      return getNode().getInputs().values();
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
  public static final class CompositeTransformVertex extends TransformVertex {
    private final Map<PValue, TransformVertex> pValueToProducer = new HashMap<>();
    private final Map<PValue, TransformVertex> pValueToConsumer = new HashMap<>();
    private final DAGBuilder<TransformVertex, DataFlowEdge> builder = new DAGBuilder<>();
    private DAG<TransformVertex, DataFlowEdge> dag = null;
    private CompositeTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
    }
    private void build() {
      if (dag != null) {
        throw new RuntimeException("DAG already have been built.");
      }
      dag = builder.build();
    }
    private void addVertex(final TransformVertex vertex) {
      vertex.getPValuesProduced().forEach(value -> pValueToProducer.put(value, vertex));
      vertex.getPValuesConsumed().forEach(value -> pValueToConsumer.put(value, vertex));
      builder.addVertex(vertex);
    }
    private void addEdge(final DataFlowEdge edge) {
      builder.connectVertices(edge);
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
  static final class DataFlowEdge extends Edge<TransformVertex> {
    private final PValue pValue;
    private final PrimitiveTransformVertex primitiveProducer;
    private final CompositeTransformVertex mostCloseCommonParent;
    private final PrimitiveTransformVertex primitiveConsumer;

    private DataFlowEdge(final PValue pValue,
                         final PrimitiveTransformVertex src,
                         final CompositeTransformVertex mostCloseCommonParent,
                         final PrimitiveTransformVertex dst) {
      super(UUID.randomUUID().toString(),
          mostCloseCommonParent.getProducerOf(pValue), mostCloseCommonParent.getConsumerOf(pValue));
      this.pValue = pValue;
      this.primitiveProducer = src;
      this.mostCloseCommonParent = mostCloseCommonParent;
      this.primitiveConsumer = dst;
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
    }
  }
}
