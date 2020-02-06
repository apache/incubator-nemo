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
package org.apache.nemo.common.ir.executionproperty;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

/**
 * Test {@link ExecutionPropertyMap}.
 */
public class ExecutionPropertyMapTest {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex destination = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final CommunicationPatternProperty.Value comPattern = CommunicationPatternProperty.Value.ONE_TO_ONE;
  private final IREdge edge = new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, source, destination);

  private ExecutionPropertyMap<EdgeExecutionProperty> edgeMap;
  private ExecutionPropertyMap<VertexExecutionProperty> vertexMap;

  @Before
  public void setUp() {
    this.edgeMap = ExecutionPropertyMap.of(edge, CommunicationPatternProperty.Value.ONE_TO_ONE);
    this.vertexMap = ExecutionPropertyMap.of(source);
  }

  @Test
  public void testDefaultValues() {
    assertEquals(comPattern, edgeMap.get(CommunicationPatternProperty.class).get());
    assertEquals(edge.getId(), edgeMap.getId());
    assertEquals(source.getId(), vertexMap.getId());
  }

  @Test
  public void testPutGetAndRemove() {
    edgeMap.put(DataStoreProperty.of(DataStoreProperty.Value.MEMORY_STORE));
    assertEquals(DataStoreProperty.Value.MEMORY_STORE, edgeMap.get(DataStoreProperty.class).get());
    edgeMap.put(DataFlowProperty.of(DataFlowProperty.Value.PULL));
    assertEquals(DataFlowProperty.Value.PULL, edgeMap.get(DataFlowProperty.class).get());
    edgeMap.put(EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY));
    assertEquals(EncoderFactory.DUMMY_ENCODER_FACTORY, edgeMap.get(EncoderProperty.class).get());
    edgeMap.put(DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY));
    assertEquals(DecoderFactory.DUMMY_DECODER_FACTORY, edgeMap.get(DecoderProperty.class).get());

    edgeMap.remove(DataFlowProperty.class);
    assertFalse(edgeMap.get(DataFlowProperty.class).isPresent());

    vertexMap.put(ParallelismProperty.of(100));
    assertEquals(100, vertexMap.get(ParallelismProperty.class).get().longValue());
  }

  @Test
  public void testEquality() {
    final ExecutionPropertyMap<ExecutionProperty> map0 = new ExecutionPropertyMap<>("map0");
    final ExecutionPropertyMap<ExecutionProperty> map1 = new ExecutionPropertyMap<>("map1");
    assertTrue(map0.equals(map1));
    assertTrue(map1.equals(map0));
    map0.put(ParallelismProperty.of(1));
    assertFalse(map0.equals(map1));
    assertFalse(map1.equals(map0));
    map1.put(ParallelismProperty.of(1));
    assertTrue(map0.equals(map1));
    assertTrue(map1.equals(map0));
    map1.put(ParallelismProperty.of(2));
    assertFalse(map0.equals(map1));
    assertFalse(map1.equals(map0));
    map0.put(ParallelismProperty.of(2));
    assertTrue(map0.equals(map1));
    assertTrue(map1.equals(map0));
    map0.put(DataFlowProperty.of(DataFlowProperty.Value.PULL));
    assertFalse(map0.equals(map1));
    assertFalse(map1.equals(map0));
    map1.put(DataFlowProperty.of(DataFlowProperty.Value.PUSH));
    assertFalse(map0.equals(map1));
    assertFalse(map1.equals(map0));
    map1.put(DataFlowProperty.of(DataFlowProperty.Value.PULL));
    assertTrue(map0.equals(map1));
    assertTrue(map1.equals(map0));
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testFinalizedProperty() {
    // this should work without a problem..
    final ExecutionPropertyMap<ExecutionProperty> map = new ExecutionPropertyMap<>("map");
    map.put(ParallelismProperty.of(1), false);
    assertEquals(ParallelismProperty.of(1), map.put(ParallelismProperty.of(2)));
    assertEquals(ParallelismProperty.of(2), map.put(ParallelismProperty.of(3), true));

    // test exception
    expectedException.expect(CompileTimeOptimizationException.class);
    map.put(ParallelismProperty.of(4));
  }
}
