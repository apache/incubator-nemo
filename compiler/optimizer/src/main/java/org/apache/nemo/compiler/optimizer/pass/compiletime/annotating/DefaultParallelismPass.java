/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;
import java.util.Optional;

/**
 * Optimization pass for tagging parallelism execution property.
 */
@Annotates(ParallelismProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class DefaultParallelismPass extends AnnotatingPass {
  private final int desiredSourceParallelism;
  // we decrease the number of parallelism by this number on each shuffle boundary.
  private final int shuffleDecreaseFactor;

  /**
   * Default constructor with desired number of source parallelism 1, shuffle decreasing factor 2.
   */
  public DefaultParallelismPass() {
    this(1, 2);
  }

  /**
   * Default constructor.
   *
   * @param desiredSourceParallelism the desired number of source parallelism.
   * @param shuffleDecreaseFactor    the parallelism decrease factor for shuffle edge.
   */
  public DefaultParallelismPass(final int desiredSourceParallelism,
                                final int shuffleDecreaseFactor) {
    super(DefaultParallelismPass.class);
    this.desiredSourceParallelism = desiredSourceParallelism;
    this.shuffleDecreaseFactor = shuffleDecreaseFactor;
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    // Propagate forward source parallelism
    dag.topologicalDo(vertex -> {
      try {
        final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
        if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
          // For source vertices, we try to split the source reader by the desired source parallelism.
          // After that, we set the parallelism as the number of split readers.
          // (It can be more/less than the desired value.)
          final SourceVertex sourceVertex = (SourceVertex) vertex;
          final Optional<Integer> originalParallelism = vertex.getPropertyValue(ParallelismProperty.class);
          // We manipulate them if it is set as default value of 1.
          if (!originalParallelism.isPresent()) {
            vertex.setProperty(ParallelismProperty.of(
              sourceVertex.getReadables(desiredSourceParallelism).size()));
          }
        } else if (!inEdges.isEmpty()) {
          // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
          // as a sideInput will have their own number of parallelism
          final Integer o2oParallelism = inEdges.stream()
            .filter(edge -> CommunicationPatternProperty.Value.ONE_TO_ONE
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get()))
            .mapToInt(edge -> edge.getSrc().getPropertyValue(ParallelismProperty.class).get())
            .max().orElse(1);
          final Integer shuffleParallelism = inEdges.stream()
            .filter(edge -> CommunicationPatternProperty.Value.SHUFFLE
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get()))
            .mapToInt(edge -> edge.getSrc().getPropertyValue(ParallelismProperty.class).get())
            .map(i -> i / shuffleDecreaseFactor)
            .max().orElse(1);
          // We set the greater value as the parallelism.
          final Integer parallelism = o2oParallelism > shuffleParallelism ? o2oParallelism : shuffleParallelism;
          vertex.setProperty(ParallelismProperty.of(parallelism));
          // synchronize one-to-one edges parallelism
          recursivelySynchronizeO2OParallelism(dag, vertex, parallelism);
        } else if (!vertex.getPropertyValue(ParallelismProperty.class).isPresent()) {
          throw new RuntimeException("There is a non-source vertex that doesn't have any inEdges "
            + "(excluding SideInput edges)");
        } // No problem otherwise.
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    return dag;
  }

  /**
   * Recursively synchronize parallelism for vertices connected by one-to-one edges.
   *
   * @param dag         the original DAG.
   * @param vertex      vertex to observe and update.
   * @param parallelism the parallelism of the most recently updated descendant.
   * @return the max value of parallelism among those observed.
   */
  static Integer recursivelySynchronizeO2OParallelism(final IRDAG dag, final IRVertex vertex,
                                                      final Integer parallelism) {
    final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
    final Integer ancestorParallelism = inEdges.stream()
      .filter(edge -> CommunicationPatternProperty.Value.ONE_TO_ONE
        .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get()))
      .map(IREdge::getSrc)
      .mapToInt(inVertex -> recursivelySynchronizeO2OParallelism(dag, inVertex, parallelism))
      .max().orElse(1);
    final Integer maxParallelism = ancestorParallelism > parallelism ? ancestorParallelism : parallelism;
    final Integer myParallelism = vertex.getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalArgumentException("No ParallelismProperty for the vertex " + vertex.getId()));

    // update the vertex with the max value.
    if (maxParallelism > myParallelism) {
      vertex.setProperty(ParallelismProperty.of(maxParallelism));
      return maxParallelism;
    }
    return myParallelism;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DefaultParallelismPass that = (DefaultParallelismPass) o;

    if (desiredSourceParallelism != that.desiredSourceParallelism) {
      return false;
    }
    return shuffleDecreaseFactor == that.shuffleDecreaseFactor;
  }

  @Override
  public int hashCode() {
    int result = desiredSourceParallelism;
    result = 31 * result + shuffleDecreaseFactor;
    return result;
  }
}
