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
package edu.snu.nemo.common.test;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.transform.Transform;

import java.util.ArrayList;
import java.util.List;

/**
 * Empty components to mock transform and source, for tests and examples.
 */
public final class EmptyComponents {
  public static final Transform EMPTY_TRANSFORM = new EmptyTransform("");

  private EmptyComponents() {
  }

  public static DAG<IRVertex, IREdge> buildEmptyDAG() {
    DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex s = new EmptyComponents.EmptySourceVertex<>("s");
    final IRVertex t1 = new OperatorVertex(new EmptyComponents.EmptyTransform("t1"));
    final IRVertex t2 = new OperatorVertex(new EmptyComponents.EmptyTransform("t2"));
    final IRVertex t3 = new OperatorVertex(new EmptyComponents.EmptyTransform("t3"));
    final IRVertex t4 = new OperatorVertex(new EmptyComponents.EmptyTransform("t4"));
    final IRVertex t5 = new OperatorVertex(new EmptyComponents.EmptyTransform("t5"));
    dagBuilder.addVertex(s);
    dagBuilder.addVertex(t1);
    dagBuilder.addVertex(t2);
    dagBuilder.addVertex(t3);
    dagBuilder.addVertex(t4);
    dagBuilder.addVertex(t5);
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, s, t1));
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, t1, t2));
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, t2, t3));
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, t3, t4));
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, t2, t5));
    return dagBuilder.build();
  }

  /**
   * An empty transform.
   * @param <I> input type.
   * @param <O> output type.
   */
  public static class EmptyTransform<I, O> implements Transform<I, O> {
    private final String name;

    /**
     * Default constructor.
     * @param name name of the empty transform.
     */
    public EmptyTransform(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final Context context, final OutputCollector<O> outputCollector) {
    }

    @Override
    public void onData(final I element) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * An empty Source Vertex.
   * @param <T> type of the data.
   */
  public static final class EmptySourceVertex<T> extends SourceVertex<T> {
    private String name;

    /**
     * Constructor.
     * @param name name for the vertex.
     */
    public EmptySourceVertex(final String name) {
      this.name = name;
    }

    /**
     * Copy Constructor for EmptySourceVertex.
     *
     * @param that the source object for copying
     */
    public EmptySourceVertex(final EmptySourceVertex that) {
      this.name = that.name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public List<Readable<T>> getReadables(final int desirednumOfSplits) {
      final List list = new ArrayList(desirednumOfSplits);
      for (int i = 0; i < desirednumOfSplits; i++) {
        list.add(new EmptyReadable<>());
      }
      return list;
    }

    @Override
    public void clearInternalStates() {
    }

    @Override
    public EmptySourceVertex<T> getClone() {
      return new EmptySourceVertex<>(this);
    }
  }

  /**
   * An empty reader.
   * @param <T> type of the data.
   */
  static final class EmptyReadable<T> implements Readable<T> {
    @Override
    public Iterable<T> read() {
      return new ArrayList<>();
    }
    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}
