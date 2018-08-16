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
 * Traverses through the given Beam pipeline to construct a DAG of Beam Transform,
 * while preserving hierarchy of CompositeTransforms.
 * This DAG will be later translated by {@link PipelineTranslator} into Nemo IR DAG.
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
        .forEach(pValue -> {
          final TransformVertex dst = getDestinationOfDataFlowEdge(vertex, pValue);
          dst.parent.addDataFlow(new DataFlowEdge(dst.parent.getProducerOf(pValue), dst));
        });
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

  /**
   * @return A vertex representing the top-level CompositeTransform.
   */
  public CompositeTransformVertex getConvertedPipeline() {
    if (rootVertex == null) {
      throw new RuntimeException("The visitor have not fully traversed through a Beam pipeline.");
    }
    return rootVertex;
  }

  /**
   * Represents a {@link org.apache.beam.sdk.transforms.PTransform} as a vertex in DAG.
   */
  public abstract class TransformVertex extends Vertex {
    private final TransformHierarchy.Node node;
    private final CompositeTransformVertex parent;

    /**
     * @param node the corresponding Beam node
     * @param parent the parent {@link CompositeTransformVertex} if any, or {@code null}
     */
    private TransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(String.format("%s%d", TRANSFORM, nextIdx++));
      this.node = node;
      this.parent = parent;
    }

    /**
     * @return Collection of {@link PValue}s this transform emits.
     */
    public abstract Collection<PValue> getPValuesProduced();

    /**
     * Searches within {@code this} to find a transform that produces the given {@link PValue}.
     *
     * @param pValue a {@link PValue}
     * @return the {@link TransformVertex} whose {@link org.apache.beam.sdk.transforms.PTransform}
     *         produces the given {@code pValue}
     */
    public abstract PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue);

    /**
     * @return the corresponding Beam node.
     */
    public TransformHierarchy.Node getNode() {
      return node;
    }

    /**
     * @return the parent {@link CompositeTransformVertex} if any, {@code null} otherwise.
     */
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

    /**
     * {@inheritDoc}
     */
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

    @Override
    public Collection<PValue> getPValuesProduced() {
      return pValuesProduced;
    }

    @Override
    public PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue) {
      if (!getPValuesProduced().contains(pValue)) {
        throw new RuntimeException();
      }
      return this;
    }

    /**
     * @return collection of {@link PValue} this transform consumes.
     */
    public Collection<PValue> getPValuesConsumed() {
      return pValuesConsumed;
    }
  }
  /**
   * Represents a transform hierarchy for composite transform.
   */
  public final class CompositeTransformVertex extends TransformVertex {
    private final Map<PValue, TransformVertex> pValueToProducer = new HashMap<>();
    private final Collection<DataFlowEdge> dataFlowEdges = new ArrayList<>();
    private final DAGBuilder<TransformVertex, DataFlowEdge> builder = new DAGBuilder<>();
    private DAG<TransformVertex, DataFlowEdge> dag = null;

    /**
     * {@inheritDoc}
     */
    private CompositeTransformVertex(final TransformHierarchy.Node node, final CompositeTransformVertex parent) {
      super(node, parent);
    }

    /**
     * Finalize this vertex and make it ready to be added to another {@link CompositeTransformVertex}.
     */
    private void build() {
      if (dag != null) {
        throw new RuntimeException("DAG already have been built.");
      }
      dataFlowEdges.forEach(builder::connectVertices);
      dag = builder.build();
    }

    /**
     * Add a {@link TransformVertex}.
     *
     * @param vertex the vertex to add
     */
    private void addVertex(final TransformVertex vertex) {
      vertex.getPValuesProduced().forEach(value -> pValueToProducer.put(value, vertex));
      builder.addVertex(vertex);
    }

    /**
     * Add a {@link DataFlowEdge}.
     *
     * @param dataFlowEdge the edge to add
     */
    private void addDataFlow(final DataFlowEdge dataFlowEdge) {
      dataFlowEdges.add(dataFlowEdge);
    }

    @Override
    public Collection<PValue> getPValuesProduced() {
      return pValueToProducer.keySet();
    }

    /**
     * Get a direct child of this vertex which produces the given {@link PValue}.
     *
     * @param pValue the {@link PValue} to search
     * @return the direct child of this vertex which produces {@code pValue}
     */
    public TransformVertex getProducerOf(final PValue pValue) {
      final TransformVertex vertex = pValueToProducer.get(pValue);
      if (vertex == null) {
        throw new RuntimeException();
      }
      return vertex;
    }

    @Override
    public PrimitiveTransformVertex getPrimitiveProducerOf(final PValue pValue) {
      return getProducerOf(pValue).getPrimitiveProducerOf(pValue);
    }

    /**
     * @return DAG of Beam hierarchy
     */
    public DAG<TransformVertex, DataFlowEdge> getDAG() {
      return dag;
    }
  }

  /**
   * Represents data flow from a transform to another transform.
   */
  public final class DataFlowEdge extends Edge<TransformVertex> {
    /**
     * @param src source vertex
     * @param dst destination vertex
     */
    private DataFlowEdge(final TransformVertex src, final TransformVertex dst) {
      super(String.format("%s%d", DATAFLOW, nextIdx++), src, dst);
    }
  }

  /**
   * @param primitiveConsumer a {@link PrimitiveTransformVertex} which consumes {@code pValue}
   * @param pValue the specified {@link PValue}
   * @return the closest {@link TransformVertex} to {@code primitiveConsumer},
   *         which is equal to or encloses {@code primitiveConsumer} and can be the destination vertex of
   *         data flow edge from the producer of {@code pValue} to {@code primitiveConsumer}.
   */
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
