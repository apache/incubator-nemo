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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link IRDAG}.
 */
public class IRDAGTest {
  private final static int MIN_THREE_SOURCE_READABLES = 3;

  private SourceVertex sourceVertex;
  private IREdge oneToOneEdge;
  private OperatorVertex firstOperatorVertex;
  private IREdge shuffleEdge;
  private OperatorVertex secondOperatorVertex;

  private IRDAG irdag;

  @Before
  public void setUp() throws Exception {
    sourceVertex = new EmptyComponents.EmptySourceVertex("source", MIN_THREE_SOURCE_READABLES);
    firstOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("first"));
    secondOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("second"));

    oneToOneEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne, sourceVertex, firstOperatorVertex);
    shuffleEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, firstOperatorVertex, secondOperatorVertex);

    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<IRVertex, IREdge>()
      .addVertex(sourceVertex)
      .addVertex(firstOperatorVertex)
      .addVertex(secondOperatorVertex)
      .connectVertices(oneToOneEdge)
      .connectVertices(shuffleEdge);
    irdag = new IRDAG(dagBuilder.build());
  }

  private void mustPass() {
    final IRDAGChecker.CheckerResult checkerResult = irdag.checkIntegrity();
    if (!checkerResult.isPassed()) {
      throw new RuntimeException("Expected pass, but failed due to ==> " + checkerResult.getFailReason());
    }
  }

  private void mustFail() {
    assertFalse(irdag.checkIntegrity().isPassed());
  }

  @Test
  public void testParallelismSuccess() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(2));
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 3),
      HashRange.of(3, MIN_THREE_SOURCE_READABLES)))));

    mustPass();
  }

  @Test
  public void testParallelismSource() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1)); // this causes failure
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1));

    mustFail();
  }

  @Test
  public void testParallelismCommPattern() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1)); // this causes failure
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 2));

    mustFail();
  }

  @Test
  public void testParallelismPartitionSet() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // this causes failure (only 2 KeyRanges < 3 parallelism)
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)
    ))));

    mustFail();
  }

  @Test
  public void testParallelismResourceSite() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // must pass
    final HashMap<String, Integer> goodSite = new HashMap<>();
    goodSite.put("SiteA", 1);
    goodSite.put("SiteB", MIN_THREE_SOURCE_READABLES - 1);
    firstOperatorVertex.setProperty(ResourceSiteProperty.of(goodSite));
    mustPass();

    // must fail
    final HashMap<String, Integer> badSite = new HashMap<>();
    badSite.put("SiteA", 1);
    badSite.put("SiteB", MIN_THREE_SOURCE_READABLES - 2);
    firstOperatorVertex.setProperty(ResourceSiteProperty.of(goodSite));
    mustFail();
  }

  @Test
  public void testParallelismResourceAntiAffinity() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // must pass
    final HashSet<Integer> goodSet = new HashSet<>();
    goodSet.add(0);
    goodSet.add(MIN_THREE_SOURCE_READABLES - 1);
    firstOperatorVertex.setProperty(ResourceAntiAffinityProperty.of(goodSet));
    mustPass();

    // must fail
    final HashSet<Integer> badSet = new HashSet<>();
    badSet.add(MIN_THREE_SOURCE_READABLES + 1);
    firstOperatorVertex.setProperty(ResourceAntiAffinityProperty.of(badSet));
    mustFail();
  }

  @Test
  public void testXX() {
    // simple test case
  }

  @Test
  public void testStreamVertex() {
    /*
    // simple test case
    // insert
    // delete
    MessageBarrierVertex;
    StreamVertex;
    SamplingVertex;
    */
  }

  @Test
  public void testRandomCalls() {
    IntStream.range(0, 100).boxed().forEach(seed -> {
      final Random random = new Random(seed);
      // (1) Randomly insert vertices
      // (2) Randomly annotate user-configurable properties
      // (3) Randomly delete vertices
      //
      // Actually execute...?
      //
      // user-configurable execution properties
      // Checker checks this...
      // IRDAGBuilder(?)
    });
  }
}
